// (C) 2023 Martin Huenniger

#pragma once

#include "log.hpp"
#include "types.hpp"

#include <memory>

namespace consensus
{

template<typename ... Class>
struct TypeList {};

template<typename Func, typename BaseClass, typename DerivedClass>
void caller(Func f, BaseClass const & b)
{
    DerivedClass* d = dynamic_cast<DerivedClass*>(&b);
    if(d != nullptr)
        f(*d);
}

template<typename Func, typename BaseClass, typename ... DerivedClass>
void cast_and_call(Func f, BaseClass const & b, TypeList<DerivedClass...>)
{
    (caller<DerivedClass>(f,b), ...);
}

class MessageBase
{
public:
    virtual ~MessageBase() = default;
    virtual IdType   getId() const = 0;
    virtual TermType getTerm() const = 0;
    virtual std::unique_ptr<MessageBase> clone() const = 0;

    template<typename T>
    
    T const* cast() const { return dynamic_cast<T const*>(this); }
    template <typename T>
    bool is_of_type() const { return cast<T>() != nullptr; }
};

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

    virtual std::unique_ptr<MessageBase> clone() const final { return std::make_unique<VoteRequest>(*this); };

    IdType getId() const final { return id; }
    TermType getTerm() const final { return term; }
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
};

using MessageTypes = TypeList<VoteRequest, VoteResponse>;

}