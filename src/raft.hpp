// (C) 2023 Martin Huenniger

#pragma once

#include "log.hpp"
#include "message.hpp"
#include "node_base.hpp"
#include "communicator_base.hpp"
#include "types.hpp"

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
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

    bool m_electing {false};
    bool m_tainted {true};
    IdType m_id {0};

    std::deque<std::unique_ptr<MessageBase>> m_message_queue;

    ApplicationCallbackType appCallback {[](LogEntry const&) {}};
public:

    RaftNode(CommunicatorBase & comm);

    ~RaftNode() override;

    IdType getId() const final;
    void setId(IdType id) final;
    std::size_t getAckedLength() const final;
    void setAckedLength(std::size_t length) final;
    std::size_t getSentLength() const final;
    void setSentLength(std::size_t length) final;
    
    void broadcast(MessageBase const & msg) final;

    void receive(MessageBase const & msg) final;

    void send(MessageBase const & msg) final;

    void setAppCallback(ApplicationCallbackType f) final;

    std::string write() const final;

    void start() final;

    void stop() final;

private:

    std::size_t quorum() const;

    std::size_t acks(std::size_t length) const;

    void replicate_log(IdType id, IdType followerId);

    void append_entries(std::size_t prefix_length, std::size_t leader_commit, LogType const & suffix);

    void commit_log_entries();

    void initialize ();

    void recovery();

    void initialize_election();

    bool voted_for( IdType id );

    void start_election_timer();

    void cancel_election_timer();

    void receive(VoteRequest const & msg);

    void receive(VoteResponse const & msg);

    void receive(LogRequest const & msg);

    void receive(LogResponse const & msg);

    template<typename T>
    void receive(T const & msg)
    {
        std::cout << __PRETTY_FUNCTION__ << ", [" << *this << "] receiving: " << msg << std::endl;
        m_log.emplace_back(m_current_term, msg);
    }

    void heartbeat();

    void thread_function();
};


}