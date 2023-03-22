// (C) 2023 Martin Huenniger

#pragma once

#include "log.hpp"
#include "types.hpp"

#include <memory>
#include <ostream>
#include <sstream>

namespace consensus
{

template<typename ... Class>
struct TypeList {};

template<typename Func, typename BaseClass, typename DerivedClass>
void caller(Func f, BaseClass const & b)
{
    DerivedClass const * d = dynamic_cast<DerivedClass const *>(&b);
    if(d != nullptr)
        f(*d);
}

template<typename Func, typename BaseClass, typename ... DerivedClass>
void cast_and_call(Func f, BaseClass const & b, TypeList<DerivedClass...>)
{
    (caller<Func, BaseClass, DerivedClass>(f,b), ...);
}

class MessageBase
{
public:
    virtual ~MessageBase() = default;
    virtual IdType   getId() const = 0;
    virtual TermType getTerm() const = 0;
    virtual std::unique_ptr<MessageBase> clone() const = 0;
    virtual std::string write() const = 0;
};

std::ostream & operator<<(std::ostream & os, MessageBase const & m);

class VoteRequest : public MessageBase
{
public:
    IdType      id {NoneId};
    TermType    term {0};
    std::size_t log_size {0};
    TermType    last_term {0};

    VoteRequest(IdType id_, TermType term_, std::size_t log_size_, TermType last_term_ ) :
        id(id_), term(term_), log_size(log_size_), last_term(last_term_)
    {}

    std::unique_ptr<MessageBase> clone() const final { return std::make_unique<VoteRequest>(*this); };

    IdType getId() const final { return id; }
    TermType getTerm() const final { return term; }
    std::string write() const final 
    { 
        std::stringstream ss;
        ss << "VoteRequest: " << id << ", " << term << ", " << log_size << ", " << last_term;
        return ss.str();
    }
};

class VoteResponse : public MessageBase
{
public:
    IdType      id {NoneId};
    TermType    term {0};
    bool        granted {false};

    VoteResponse( IdType id_, TermType term_, bool granted_) : 
        id(id_), term(term_), granted(granted_)
    {}
    virtual std::unique_ptr<MessageBase> clone() const final { return std::make_unique<VoteResponse>(*this); };

    IdType getId() const final { return id; }
    TermType getTerm() const final { return term; }
    std::string write() const final 
    { 
        std::stringstream ss;
        ss << "VoteResponse: " << id << ", " << term << ", " << granted;
        return ss.str();
    }

};

class LogRequest : public MessageBase
{
public:
    IdType      id {NoneId};
    TermType    term {0};
    std::size_t prefix_length {0};
    TermType    prefix_term {0};
    std::size_t commit_length {0};
    LogType     log {0};

    LogRequest(IdType id_, TermType term_, std::size_t prefix_length_, TermType prefix_term_, std::size_t commit_length_, LogType const & log_) : 
        id(id_), term(term_), prefix_length(prefix_length_), prefix_term(prefix_term_), commit_length(), log(log_)
    {}
    virtual std::unique_ptr<MessageBase> clone() const final { return std::make_unique<LogRequest>(*this); };

    IdType getId() const final { return id; }
    TermType getTerm() const final { return term; }
    std::string write() const final 
    { 
        std::stringstream ss;
        ss << "LogRequest: " << id << ", " << term << ", " << prefix_length << ", " << prefix_term << ", " << commit_length << ", [" << log << "]";
        return ss.str();
    }

};

class LogResponse : public MessageBase
{
public:
    IdType      id {NoneId};
    TermType    term {0};
    std::size_t ack {0};
    bool        success {false};

    LogResponse(IdType id_, TermType term_, std::size_t ack_, bool success_) : 
        id(id_), term(term_), ack(ack_), success(success_)
    {}
    virtual std::unique_ptr<MessageBase> clone() const final { return std::make_unique<LogResponse>(*this); };

    IdType getId() const final { return id; }
    TermType getTerm() const final { return term; }
    std::string write() const final 
    { 
        std::stringstream ss;
        ss << "LogResponse: " << id << ", " << term << ", " << ack << ", " << success;
        return ss.str();
    }
};

class StringMessage : public MessageBase
{
public:
    IdType      id {NoneId};
    TermType    term {0};
    std::string string {};

    StringMessage(std::string const & string_) : 
        string(string_)
    {}

    virtual std::unique_ptr<MessageBase> clone() const final { return std::make_unique<StringMessage>(*this); };

    IdType getId() const final { return 0; }
    TermType getTerm() const final { return 0; }
    std::string write() const final 
    { 
        std::stringstream ss;
        ss << "StringMessage: " << string;
        return ss.str();
    }
    
};

using MessageTypes = TypeList<VoteRequest, VoteResponse, LogRequest, LogResponse, StringMessage>;

}