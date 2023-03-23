// (C) 2023 Martin Huenniger

#include "raft.hpp"
#include "log.hpp"
#include "message.hpp"
#include "node_base.hpp"
#include "types.hpp"


#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <deque>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>


namespace consensus 
{


    RaftNode::RaftNode(CommunicatorBase & comm) : 
        m_comm(comm)
    {
        initialize();
        m_comm.AddNode(*this);
    }

    RaftNode::~RaftNode()
    {
        if(running)
            stop();
    }

    //virtual std::unique_ptr<NodeBase> clone() const { return std::make_unique<RaftNode>(*this); }

    IdType RaftNode::getId() const 
    { 
        return m_id; 
    }

    void RaftNode::setId(IdType id)
    { 
        m_id = id; 
    }
    
    void RaftNode::broadcast(MessageBase const & msg)
    {
        std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] broadcasting: " << msg << std::endl;

        if(m_current_role == Role::Leader)
        {
            m_log.emplace_back(LogEntry(m_current_term, msg));
            auto acked_length = m_log.size();
            m_comm.for_each([this](NodeBase& node){
                if(node.getId() != getId())
                    replicate_log(getId(), node.getId());
            });
        }
        else 
        {
            // forward the request to the leader via FIFO
            m_comm.send(msg, m_current_leader);
        }
    }

    void RaftNode::receive(MessageBase const & msg) 
    {
        auto f = [this](auto const & msg) -> void { receive(msg); };
        cast_and_call(f, msg, MessageTypes{});
    }

    void RaftNode::send(MessageBase const & msg) 
    {
        std::lock_guard<std::mutex> lock(m_queue_mutex);
        m_message_queue.push_front(msg.clone());
    }

    void RaftNode::setAppCallback(ApplicationCallbackType f) 
    { 
        appCallback = f; 
    }

    std::string RaftNode::write() const
    {
        const auto role = [this]() -> std::string
        {
            switch(m_current_role)
            {
                case Role::Leader: return "Leader";
                case Role::Follower: return "Follower";
                case Role::Candidate: return "Candidate";
                default: return "blubb";
            }
        };
        std::stringstream ss;
        ss << "ID: " << m_id << ", term: " << m_current_term << ", role: " << role() << ", voted: " << m_voted_for << ", leader: " << m_current_leader << ", log: [" << m_log << "]";
        return ss.str(); 
    }

    void RaftNode::start()
    {
        running.store(true);
        m_thread = std::thread([this](){
            while(running.load())
            {
                thread_function();
            }
        });
    }

    void RaftNode::stop()
    {
        running.store(false);
        m_thread.join();
        m_tainted = true;
    }

    std::size_t RaftNode::quorum() const { return std::ceil( double(m_comm.numNodes() + 1) / 2.0 ); }

    std::size_t RaftNode::acks(std::size_t length) const
    {
        std::size_t result {0}; 
        for(auto const & [id, acks] : m_acked_length)
            if(acks >= length)
                ++result;
        // std::cout << "acks(" << length << ") = " << result << std::endl;
        return result;
    }

    void RaftNode::replicate_log(IdType id, IdType followerId)
    {
        //std::cout << __PRETTY_FUNCTION__ << ", " << *this << std::endl;
        auto prefix_length = m_sent_length[followerId];
        LogType suffix(m_log.cbegin() + prefix_length, m_log.cend());
        // std::cout << "suffix: [" << suffix << "]" << std::endl;
        TermType prefix_term = 0;
        if(prefix_length > 0)
            prefix_term = m_log.back().term;
        // std::cout << "Sending LogRequest" << std::endl;
        m_comm.send(LogRequest(id, m_current_term, prefix_length, prefix_term, m_commit_length, suffix), 
                     followerId);
    }

    void RaftNode::append_entries(std::size_t prefix_length, std::size_t leader_commit, LogType const & suffix) 
    {
        // std::cout << __PRETTY_FUNCTION__ << ", " << *this << std::endl;
        // std::cout << "suffix: [" << suffix << "]" << std::endl;
        if(!suffix.empty() && m_log.size() > prefix_length)
        {
            // std::cout << "1111111111111111111" <<std::endl;
            auto index = std::min(m_log.size(), prefix_length + suffix.size()) - 1;
            if(m_log[index].term != suffix[index - prefix_length].term)
            {
                m_log = LogType(m_log.begin(), m_log.begin() + prefix_length-1);
            }
        }
        if(prefix_length + suffix.size() > m_log.size() )
        {
            // std::cout << "222222222222222222222" << std::endl;
            for(std::size_t i {m_log.size() - prefix_length}; i < suffix.size(); ++i)
            {
                // std::cout << "suffix[" << i <<"] = " << suffix[i] << std::endl;
                m_log.emplace_back(suffix[i]);
            }
            //std::cout << m_log << std::endl;
        }
        // std::cout << "Leader commit " << leader_commit << ", m_commit_length: " << m_commit_length << std::endl;
        if(leader_commit > m_commit_length)
        {
            // std::cout << "=================================================" << std::endl;
            for(auto it = m_log.cbegin() + m_commit_length; it != m_log.cend(); ++it)
            {
                appCallback(*it);
            }
        }
    }

    void RaftNode::commit_log_entries()
    {
        // std::cout << __PRETTY_FUNCTION__ << ", " << *this << std::endl;
        auto minAcks = quorum();
        std::vector<std::size_t> ready {};
        for(std::size_t len {1}; len < m_log.size(); ++len)
            if(acks(len) >= minAcks)
                ready.push_back(len);
        // std::cout << "======================================" << (ready.empty() ? "No readies" : "Readies") << std::endl;
        if(!ready.empty() && (ready.back() > m_commit_length) && (m_log[ready.back()].term == m_current_term))
        {
            // std::cout << "+++++++++++++++++++++++++++" << std::endl;
            for(auto i {m_commit_length}; i < ready.back(); ++i)
                appCallback(m_log[i]);
            m_commit_length = ready.back();
        }
    }

    void RaftNode::initialize ()
    {
        // on recovery from crash
        recovery();

        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> dist( 150, 300);
        m_election_timeout = std::chrono::milliseconds(dist(rng));
    }

    void RaftNode::recovery()
    {
        m_current_role = Role::Follower;
        m_current_leader = NoneId;
        m_votes_received = {};
        m_sent_length = {};
        m_acked_length = {};
        m_tainted = false;
    }

    void RaftNode::initialize_election()
    {
        m_current_term += 1;
        m_current_role = Role::Candidate;
        m_voted_for = getId();
        m_votes_received = {getId()};

        TermType last_term = 0;
        if(!m_log.empty())
            last_term = m_log.back().term;

        m_comm.send(VoteRequest(getId(), m_current_term, m_log.size(), last_term));

        start_election_timer();
    }

    bool RaftNode::voted_for( IdType id )
    {
        return m_voted_for == NoneId || m_voted_for == id;
    }

    void RaftNode::start_election_timer() 
    { 
        m_election_timer = std::chrono::steady_clock::now();
        m_electing = true;
    };

    void RaftNode::cancel_election_timer() 
    { 
        m_election_timer = {}; 
        m_electing = false;
        m_heartbeat_timer = std::chrono::steady_clock::now();
    };

    void RaftNode::receive(VoteRequest const & msg)
    {
        //std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
        if(msg.getTerm() > m_current_term)
        {
            m_current_term = msg.getTerm();
            m_current_role = Role::Follower;
            m_voted_for = NoneId;
        }
        
        TermType lastTerm = 0;
        if(!m_log.empty())
            lastTerm = m_log.back().term;
        
        bool log_ok = (msg.getTerm() > lastTerm) || (msg.getTerm() == lastTerm && msg.log_size > m_log.size());

        if((msg.getTerm() == m_current_term) && log_ok && voted_for(msg.getId()))
        {
            m_voted_for = msg.getId();
            m_comm.send(VoteResponse( getId(), m_current_term, true ), 
                         msg.getId());
        }
        else 
        {
            m_comm.send(VoteResponse(getId(), m_current_term, false ), 
                         msg.getId());
        }
    }

    void RaftNode::receive(VoteResponse const & msg)
    {
        //std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
        if(m_current_role == Role::Candidate && msg.getTerm() == m_current_term && msg.granted)
        {
            m_votes_received.insert(msg.getId());
            // std::cout << "Id: " << m_id << ", " << m_votes_received.size() <<" Votes received: ";
            // for(auto const & vote : m_votes_received)
            //     std::cout << vote << " ";
            // std::cout << std::endl;
            if(m_votes_received.size() >= quorum())
            {
                m_current_role = Role::Leader;
                m_current_leader = getId();
                cancel_election_timer();
                m_comm.for_each([this](NodeBase & node){
                    if(node.getId() != getId())
                        replicate_log(getId(), node.getId());
                });
                std::cout << *this << " is Leader and done electing" << std::endl;
            }
        }
        else if(msg.getTerm() > m_current_term)
        {
            m_current_term = msg.getTerm();
            m_current_role = Role::Follower;
            m_voted_for = NoneId;
            cancel_election_timer();
            std::cout << *this << " assumes to be Follower and done electing" << std::endl;
        }
    }

    void RaftNode::receive(LogRequest const & msg)
    {
        //std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
        if(msg.getTerm() > m_current_term)
        {
            m_current_term = msg.getTerm();
            m_voted_for = NoneId;
            cancel_election_timer();
            std::cout << *this << " is done electing" << std::endl;
        }
        if(msg.getTerm() == m_current_term)
        {
            m_current_role = Role::Follower;
            m_current_leader = msg.getId();
        }
        bool log_ok = (m_log.size() >= msg.prefix_length) && (msg.prefix_length == 0 || m_log[msg.prefix_length-1].term == msg.prefix_term);
        //std::cout << *this << (log_ok ? " Log ok" : " Log not ok" ) << " " << m_log.size() << " >= " << msg.prefix_length;
        //if(msg.prefix_length > 0) std::cout << " && " << m_log[msg.prefix_length-1].term << " == " << msg.prefix_term  << " " << msg << std::endl;
        //if(msg.prefix_length > 0) std::cout << "================>>>>>>>>" << m_log << std::endl;
        if(msg.getTerm() == m_current_term && log_ok)
        {
            append_entries(msg.prefix_length, msg.commit_length, msg.log);
            auto ack = msg.prefix_length + msg.log.size();
            m_comm.send(LogResponse(getId(), m_current_term, ack, true), m_current_leader);
        }
        else 
        {
            m_comm.send(LogResponse(getId(), m_current_term, 0, false), m_current_leader);
        }
    }

    void RaftNode::receive(LogResponse const & msg)
    {
        //std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
        if(msg.getTerm() == m_current_term && m_current_role == Role::Leader)
        {
            //std::cout << "//////////////////////////////////" <<(msg.success ? "yay!" : "no!") << ", Ack: " << msg.ack << ", " << m_acked_length[msg.getId()] << std::endl;
            if(msg.success && msg.ack >= m_acked_length[msg.getId()])
            {
                m_sent_length[msg.getId()] = msg.ack;
                m_acked_length[msg.getId()] = msg.ack;
                // std::cout << "................." << m_sent_length[msg.getId()] << ", " << m_acked_length[msg.getId()] << std::endl;
                commit_log_entries();
            }
            else if(m_sent_length[msg.getId()] > 0)
            {
                m_sent_length[msg.getId()] = m_sent_length[msg.getId()] - 1;
                replicate_log(getId(), msg.getId());
            }
        }
        else if(msg.term > m_current_term)
        {
            //std::cout << "\\\\\\\\\\\\\\\\\\\\\\\\\\" << std::endl;
            m_current_term = msg.getTerm();
            m_current_role = Role::Follower;
            m_voted_for = NoneId;
            cancel_election_timer();
            std::cout << *this << " is done electing" << std::endl;
        }
    }

    void RaftNode::heartbeat() 
    {
        if(m_current_role == Role::Leader)
        {
            m_comm.for_each([this](NodeBase & node){
                if(node.getId() != getId())
                    replicate_log(getId(), node.getId());
            });
        }
        m_heartbeat_timer = std::chrono::steady_clock::now();
    }

    void RaftNode::thread_function()
    {
        if(m_tainted)
        {
            //std::cout << *this << " is tainted" << std::endl;
            recovery();
        }

        if(m_electing)
        {
            auto now = std::chrono::steady_clock::now();
            auto timeout = (now - m_election_timer) > m_election_timeout;
            auto no_vote = m_voted_for == NoneId;
            if(timeout || no_vote)  
            {
                //std::cout << *this << " is electing, because: " << (timeout ? "timeout " : "") << (no_vote ? "not voted for anyone" : "") << std::endl;
                initialize_election();  
                m_heartbeat_timer = std::chrono::steady_clock::now();
            } 
        } 
        else 
        {            
            if(m_current_leader == NoneId)
            {
                //std::cout << *this << " has no leader starting election timer" << std::endl;
                start_election_timer();
            }

            auto now = std::chrono::steady_clock::now();
            if(now - m_heartbeat_timer > std::chrono::seconds(5))
                heartbeat();                
        }

        if(!m_message_queue.empty())
        {
            std::unique_ptr<MessageBase> msg;
            {
                std::lock_guard<std::mutex> lock(m_queue_mutex);
                msg = m_message_queue.back()->clone();
                m_message_queue.pop_back();
            };
            receive(*msg);
        }
    }

}