// (C) 2023 Martin Huenniger

#pragma once

#include "message.hpp"
#include "log.hpp"
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


class NodeBase
{
public:
    virtual ~NodeBase() = default;
    virtual std::unique_ptr<NodeBase> clone() const = 0;
    virtual void receive(MessageBase const & msg) = 0;
    virtual IdType getId() const = 0;
    virtual void setId(IdType id)  = 0;
    virtual std::size_t getAckedLength() const = 0;
    virtual void setAckedLength(std::size_t) = 0;
    virtual std::size_t getSentLength() const = 0;
    virtual void setSentLength(std::size_t) = 0;
private:
};

class NodeManager
{
public:
    using ContainerType = std::vector<std::unique_ptr<NodeBase>>;
    using const_iterator = ContainerType::const_iterator;
    using iterator       = ContainerType::iterator;

    iterator begin() { return m_nodes.begin(); }
    iterator end() { return m_nodes.end(); }
    const_iterator begin() const { return m_nodes.begin(); }
    const_iterator end() const { return m_nodes.end(); }

    void AddNode(NodeBase & node)
    {
        m_nodes.emplace_back(std::move(node));
        m_nodes.back()->setId(m_nodes.size());
    }

    void send(MessageBase const & msg, NodeBase & node)
    {
        node.receive(msg);
    }

    void send(MessageBase const & msg)
    {
        for(auto & node : m_nodes)
            send(msg, *node);
    }

    void send(MessageBase const & msg, IdType id)
    {
        send(msg, *m_nodes[id]);
    }

    std::size_t numNodes() const {return m_nodes.size(); }

    std::size_t quorum() const { return std::ceil( double(m_nodes.size() + 1) / 2.0 ); }

    std::size_t getAckedLength(IdType id) { return m_nodes[id]->getAckedLength(); }
    void setAckedLength(IdType id, std::size_t length) { m_nodes[id]->setAckedLength(length); }

    std::size_t getSentLength(IdType id) { return m_nodes[id]->getSentLength(); }
    void setSentLength(IdType id, std::size_t length) { m_nodes[id]->setSentLength(length); }

    std::size_t acks(std::size_t length) const
    {
        std::size_t result {0}; 
        for(auto const & node : m_nodes)
            if(node->getAckedLength() > length)
                ++result;
        return result;
    }

private:
    ContainerType m_nodes;
};

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
    NodeManager m_nodes {};
    IdType m_id;

    ApplicationCallbackType appCallback {[](LogEntry const&) {}};
public:

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

    void replicate_log(IdType id, IdType followerId)
    {
        auto prefix_length = m_nodes.getSentLength(followerId);
        LogType suffix(m_log.cbegin() + prefix_length, m_log.cend());
        TermType prefix_term = 0;
        if(prefix_term > 0)
            prefix_term = m_log.back().term;
        m_nodes.send(LogRequest(id, m_current_term, prefix_length, prefix_term, m_commit_length, suffix), 
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
        auto minAcks = m_nodes.quorum();
        std::vector<std::size_t> ready {};
        for(std::size_t len {1}; len < m_log.size(); ++len)
            if(m_nodes.acks(len) > minAcks)
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

        // on nodeId suspects leader has failed or election timeout
        //IdType nodeId;
        m_current_term += 1;
        m_current_role = Role::Candidate;
        m_voted_for = getId();
        m_votes_received = {getId()};
        TermType last_term = 0;

        if(!m_log.empty())
            last_term = m_log.back().term;

        m_nodes.send(VoteRequest(getId(), m_current_term, m_log.size(), last_term));

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
            m_nodes.send(VoteResponse( getId(), m_current_term, true ), 
                         msg.getId());
        }
        else 
        {
            m_nodes.send(VoteResponse(getId(), m_current_term, false ), 
                         msg.getId());
        }
    }

    void receive(VoteResponse const & msg)
    {
        if(m_current_role == Role::Candidate && msg.getTerm() == m_current_term && msg.granted)
        {
            m_votes_received.insert(msg.getId());
            if(m_votes_received.size() >= m_nodes.quorum())
            {
                m_current_role = Role::Leader;
                m_current_leader = getId();
                cancel_election_timer();
                for(auto & node : m_nodes)
                    if(node->getId() != getId())
                        replicate_log(getId(), node->getId());
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
        if(msg.getTerm() == m_current_term)
        {
            append_entries(msg.prefix_length, msg.commit_length, msg.log);
            auto ack = msg.prefix_length + msg.log.size();
            m_nodes.send(LogResponse(getId(), m_current_term, ack, true), getId());
        }
        else 
        {
            m_nodes.send(LogResponse(getId(), m_current_term, 0, false), getId());
        }
    }

    void receive(LogResponse const & msg)
    {
        if(msg.getTerm() == m_current_term && m_current_role == Role::Leader)
        {
            if(msg.success && msg.ack >= m_nodes.getAckedLength(msg.getId()))
            {
                m_nodes.setSentLength(msg.getId(), msg.ack);
                m_nodes.setAckedLength(msg.getId(), msg.ack);
                commit_log_entries();
            }
            else if(m_nodes.getSentLength(msg.getId()) > 0)
            {
                m_nodes.setSentLength(msg.getId(), m_nodes.getSentLength(msg.getId()));
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
            for(auto & node : m_nodes)
                if(node->getId() != getId())
                    replicate_log(getId(), node->getId());
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
            for(auto & node : m_nodes)
                if(node->getId() != getId())
                    replicate_log(getId(), node->getId());
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