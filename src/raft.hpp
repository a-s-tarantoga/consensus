// (C) 2023 Martin Huenniger

#pragma once

#include "log.hpp"
#include "message.hpp"
#include "node_base.hpp"
#include "node_manager.hpp"
#include "types.hpp"


#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <unordered_set>


namespace consensus 
{


using ApplicationCallbackType = std::function<void(LogEntry const &)>;

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

    bool running {false};
    CommunicatorBase & m_comm;
    IdType m_id;

    ApplicationCallbackType appCallback {[](LogEntry const&) {}};
public:

    RaftNode(CommunicatorBase & comm) : 
        m_comm(comm)
    {
        initialize();
        initialize_election();
        m_comm.AddNode(*this);
    }

    virtual std::unique_ptr<NodeBase> clone() const { return std::make_unique<RaftNode>(*this); }

    virtual IdType getId() const final { return m_id; }
    virtual void setId(IdType id) final { m_id = id; }
    virtual std::size_t getAckedLength() const final { return m_acked_length; }
    virtual void setAckedLength(std::size_t length) final { m_acked_length = length; }
    virtual std::size_t getSentLength() const final { return m_sent_length; }
    virtual void setSentLength(std::size_t length) final { m_sent_length = length; }
    
    void receive(MessageBase const & msg)
    {
        auto f = [this](auto const & msg) -> void { receive(msg); };
        cast_and_call(f, msg, MessageTypes{});
    }

    void setAppCallback(ApplicationCallbackType f) { appCallback = f; }

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
        auto prefix_length = m_comm.getSentLength(followerId);
        LogType suffix(m_log.cbegin() + prefix_length, m_log.cend());
        TermType prefix_term = 0;
        if(prefix_term > 0)
            prefix_term = m_log.back().term;
        m_comm.send(LogRequest(id, m_current_term, prefix_length, prefix_term, m_commit_length, suffix), 
                     followerId);
    }

    void append_entries(std::size_t prefix_length, std::size_t leader_commit, LogType const & suffix) 
    {
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
        m_current_role = Role::Follower;
        m_current_leader = NoneId;
        m_votes_received = {};
        m_sent_length = 0;
        m_acked_length = 0;
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

    void start_election_timer() {};
    void cancel_election_timer() {};

    void receive(VoteRequest const & msg)
    {
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

        if(msg.getTerm() == m_current_term && log_ok && voted_for(msg.getId()))
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
        if(m_current_role == Role::Candidate && msg.getTerm() == m_current_term && msg.granted)
        {
            m_votes_received.insert(msg.getId());
            if(m_votes_received.size() >= quorum())
            {
                m_current_role = Role::Leader;
                m_current_leader = getId();
                cancel_election_timer();
                m_comm.for_each([this](NodeBase & node){
                    if(node.getId() != getId())
                        replicate_log(getId(), node.getId());
                });
            }
        }
        else if(msg.getTerm() > m_current_term)
        {
            m_current_term = msg.getTerm();
            m_current_role = Role::Follower;
            m_voted_for = NoneId;
            cancel_election_timer();
        }
    }

    void receive(LogRequest const & msg)
    {
        if(msg.getTerm() > m_current_term)
        {
            m_current_term = msg.getTerm();
            m_voted_for = NoneId;
            cancel_election_timer();
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
        }
    }

    void broadcast(MessageBase const & msg) 
    {
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
        }
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
    }



    void thread_function()
    {
        while(running)
        {

        }
    }



};

}