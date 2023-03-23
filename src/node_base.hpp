// (C) 2023 Martin Huenniger

#pragma once

#include "message.hpp"

namespace consensus
{
using ApplicationCallbackType = std::function<void(LogEntry const &)>;

class NodeBase
{
public:
    virtual ~NodeBase() = default;

    virtual void broadcast(MessageBase const & msg) = 0; 
    virtual void send(MessageBase const & msg) = 0;
    virtual void receive(MessageBase const & msg) = 0;
    virtual IdType getId() const = 0;
    virtual void setId(IdType id)  = 0;
    virtual std::string write() const = 0;
    virtual void setAppCallback(ApplicationCallbackType f) = 0;
    virtual void start() = 0;
    virtual void stop() = 0;
private:
};

inline std::ostream& operator<<(std::ostream& os, NodeBase const & n)
{
    return os << n.write();
}

}