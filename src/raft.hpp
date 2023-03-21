// (C) 2023 Martin Huenniger

#pragma once

#include "log.hpp"
#include "message.hpp"
#include "node_base.hpp"
#include "node_manager.hpp"
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

class RaftNode : public NodeBase
{
    TermType     m_current_term {0};
    IdType       m_voted_for {NoneId};
    LogType      m_log {};
    std::size_t  m_commit_length {0};
    Role         m_current_role {Role::Follower};
    IdType       m_current_leader {NoneId};
    VoteListType m_votes_received {};
    std::size_t  m_sent_length {0};
    std::size_t  m_acked_length {0};

    CommunicatorBase & m_comm;
    std::atomic<bool> running {false};
    std::thread m_thread;
    std::chrono::milliseconds m_election_timeout {};
    std::chrono::steady_clock::time_point m_election_timer {};
    std::chrono::steady_clock::time_point m_heartbeat_timer {};
    std::mutex m_queue_mutex;
    std::condition_variable m_cv;
    bool m_electing {false};
    bool m_tainted {true};
    IdType m_id {0};
    std::deque<std::unique_ptr<MessageBase>> m_message_queue;

    ApplicationCallbackType appCallback {[](LogEntry const&) {}};
public:

    RaftNode(CommunicatorBase & comm) : 
        m_comm(comm)
    {
        initialize();
        m_comm.AddNode(*this);
    }

    ~RaftNode() override
    {
        if(running)
            stop();
    }

    //virtual std::unique_ptr<NodeBase> clone() const { return std::make_unique<RaftNode>(*this); }

    IdType getId() const final { return m_id; }
    void setId(IdType id) final { m_id = id; }
    std::size_t getAckedLength() const final { return m_acked_length; }
    void setAckedLength(std::size_t length) final { m_acked_length = length; }
    std::size_t getSentLength() const final { return m_sent_length; }
    void setSentLength(std::size_t length) final { m_sent_length = length; }
    
    void broadcast(MessageBase const & msg) final
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

    void receive(MessageBase const & msg) final
    {
        auto f = [this](auto const & msg) -> void { receive(msg); };
        cast_and_call(f, msg, MessageTypes{});
    }

    void send(MessageBase const & msg) final
    {
        std::lock_guard<std::mutex> lock(m_queue_mutex);
        m_message_queue.push_front(msg.clone());
    }

    void setAppCallback(ApplicationCallbackType f) final { appCallback = f; }

    std::string write() const final
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

    void start() final
    {
        running.store(true);
        m_thread = std::thread([this](){
            while(running.load())
            {
                thread_function();
            }
        });
    }

    void stop() final
    {
        running.store(false);
        m_thread.join();
        m_tainted = true;
    }

private:

    std::size_t quorum() const { return std::ceil( double(m_comm.numNodes() + 1) / 2.0 ); }

    std::size_t acks(std::size_t length) const
    {
        std::size_t result {0}; 
        m_comm.for_each([&](NodeBase const & node){
            if(node.getAckedLength() > length)
                ++result;
        });
        return result;
    }

    void replicate_log(IdType id, IdType followerId)
    {
        std::cout << __PRETTY_FUNCTION__ << ", Id: " << m_id << std::endl;
        auto prefix_length = m_comm.getSentLength(followerId);
        LogType suffix(m_log.cbegin() + prefix_length, m_log.cend());
        TermType prefix_term = 0;
        if(prefix_term > 0)
            prefix_term = m_log.back().term;
        std::cout << "Sending LogRequest" << std::endl;
        m_comm.send(LogRequest(id, m_current_term, prefix_length, prefix_term, m_commit_length, suffix), 
                     followerId);
    }

    void append_entries(std::size_t prefix_length, std::size_t leader_commit, LogType const & suffix) 
    {
        std::cout << __PRETTY_FUNCTION__ << ", Id: " << m_id << std::endl;
        if(!suffix.empty() && m_log.size() > prefix_length)
        {
            auto index = std::min(m_log.size(), prefix_length + suffix.size()) - 1;
            if(m_log[index].term != suffix[index - prefix_length].term)
            {
                m_log = LogType(m_log.begin(), m_log.begin() + prefix_length-1);
            }
        }
        if(prefix_length + suffix.size() > m_log.size())
        {
            for(std::size_t i {m_log.size() - prefix_length}; i < suffix.size(); ++i)
                m_log.emplace_back(std::move(suffix[i]));
        }
        if(leader_commit > m_commit_length)
        {
            for(auto it = m_log.cbegin() + m_commit_length; it != m_log.cend(); ++it)
            {
                appCallback(*it);
            }
        }
    }

    void commit_log_entries()
    {
        std::cout << __PRETTY_FUNCTION__ << ", Id: " << m_id << std::endl;
        auto minAcks = quorum();
        std::vector<std::size_t> ready {};
        for(std::size_t len {1}; len < m_log.size(); ++len)
            if(acks(len) > minAcks)
                ready.push_back(len);
        if(!ready.empty() && ready.back() > m_commit_length && m_log[ready.back()].term == m_current_term)
        {
            for(auto i {m_commit_length}; i < ready.back(); ++i)
                appCallback(m_log[i]);
            m_commit_length = ready.back();
        }
    }

    void initialize ()
    {
        // on recovery from crash
        recovery();

        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> dist( 150, 300);
        m_election_timeout = std::chrono::milliseconds(dist(rng));
    }

    void recovery()
    {
        m_current_role = Role::Follower;
        m_current_leader = NoneId;
        m_votes_received = {};
        m_sent_length = 0;
        m_acked_length = 0;
        m_tainted = false;
    }

    void initialize_election()
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

    bool voted_for( IdType id )
    {
        return m_voted_for == NoneId || m_voted_for == id;
    }

    void start_election_timer() 
    { 
        m_election_timer = std::chrono::steady_clock::now();
        m_electing = true;
    };

    void cancel_election_timer() 
    { 
        m_election_timer = {}; 
        m_electing = false;
        m_heartbeat_timer = std::chrono::steady_clock::now();
    };

    void receive(VoteRequest const & msg)
    {
        std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
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

    void receive(VoteResponse const & msg)
    {
        std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
        if(m_current_role == Role::Candidate && msg.getTerm() == m_current_term && msg.granted)
        {
            m_votes_received.insert(msg.getId());
            std::cout << "Id: " << m_id << ", " << m_votes_received.size() <<" Votes received: ";
            for(auto const & vote : m_votes_received)
                std::cout << vote << " ";
            std::cout << std::endl;
            if(m_votes_received.size() >= quorum())
            {
                m_current_role = Role::Leader;
                m_current_leader = getId();
                cancel_election_timer();
                m_comm.for_each([this](NodeBase & node){
                    if(node.getId() != getId())
                        replicate_log(getId(), node.getId());
                });
                std::cout << "Id " << m_id << " is Leader and done electing" << std::endl;
            }
        }
        else if(msg.getTerm() > m_current_term)
        {
            m_current_term = msg.getTerm();
            m_current_role = Role::Follower;
            m_voted_for = NoneId;
            cancel_election_timer();
            std::cout << "Id " << m_id << " assumes to be Follower and done electing" << std::endl;
        }
    }

    void receive(LogRequest const & msg)
    {
        std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
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
        bool log_ok = (m_log.size() >= msg.log.size()) && (msg.prefix_length == 0 || m_log[msg.prefix_length-1].term == msg.prefix_term);
        if(msg.getTerm() == m_current_term && log_ok)
        {
            append_entries(msg.prefix_length, msg.commit_length, msg.log);
            auto ack = msg.prefix_length + msg.log.size();
            m_comm.send(LogResponse(getId(), m_current_term, ack, true), getId());
        }
        else 
        {
            m_comm.send(LogResponse(getId(), m_current_term, 0, false), getId());
        }
    }

    void receive(LogResponse const & msg)
    {
        std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
        if(msg.getTerm() == m_current_term && m_current_role == Role::Leader)
        {
            if(msg.success && msg.ack >= m_comm.getAckedLength(msg.getId()))
            {
                m_comm.setSentLength(msg.getId(), msg.ack);
                m_comm.setAckedLength(msg.getId(), msg.ack);
                commit_log_entries();
            }
            else if(m_comm.getSentLength(msg.getId()) > 0)
            {
                m_comm.setSentLength(msg.getId(), m_comm.getSentLength(msg.getId()));
                replicate_log(getId(), msg.getId());
            }
        }
        else if(msg.term > m_current_term)
        {
            m_current_term = msg.getTerm();
            m_current_role = Role::Follower;
            m_voted_for = NoneId;
            cancel_election_timer();
            std::cout << *this << " is done electing" << std::endl;
        }
    }

    template<typename T>
    void receive(T const & msg)
    {
        std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
        m_log.emplace_back(m_current_term, msg);
    }

    void heartbeat() 
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

    void thread_function()
    {
        if(m_tainted)
        {
            std::cout << *this << " is tainted" << std::endl;
            recovery();
        }

        if(m_electing)
        {
            auto now = std::chrono::steady_clock::now();
            auto timeout = (now - m_election_timer) > m_election_timeout;
            auto no_vote = m_voted_for == NoneId;
            if(timeout || no_vote)  
            {
                std::cout << *this << " is electing, because: " << (timeout ? "timeout " : "") << (no_vote ? "not voted for anyone" : "") << std::endl;
                initialize_election();  
                m_heartbeat_timer = std::chrono::steady_clock::now();
            } 
        } 
        else 
        {            
            if(m_current_leader == NoneId)
            {
                std::cout << *this << " has no leader starting election timer" << std::endl;
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
};


}