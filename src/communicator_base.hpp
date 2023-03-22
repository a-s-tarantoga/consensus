// (C) 2023 Martin Huenniger

#pragma once

#include "node_base.hpp"

#include <functional>

namespace consensus
{

class CommunicatorBase
{
public:
    virtual ~CommunicatorBase() = default;

    virtual void AddNode(NodeBase & node) = 0;
    virtual std::size_t numNodes() const = 0;
    virtual void send(MessageBase const &) = 0;
    virtual void send(MessageBase const & msg, IdType id) = 0;

    virtual void for_each(std::function<void(NodeBase const&)> f ) const = 0;
    virtual void for_each(std::function<void(NodeBase &)> f) = 0;
    
    virtual std::size_t getAckedLength(IdType id) = 0;
    virtual void setAckedLength(IdType id, std::size_t length) = 0;
    virtual std::size_t getSentLength(IdType id) = 0;
    virtual void setSentLength(IdType id, std::size_t length) = 0;
};



}