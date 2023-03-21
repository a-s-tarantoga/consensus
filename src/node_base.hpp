// (C) 2023 Martin Huenniger

#pragma once

#include "message.hpp"

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

}