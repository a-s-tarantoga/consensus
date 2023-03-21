// (C) 2023 Martin Huenniger

#pragma once

#include "node_base.hpp"
#include "types.hpp"

#include <functional>
#include <iostream>
#include <unordered_map>

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

    virtual void for_each( std::function<void(NodeBase const&)> f ) const = 0;
    virtual void for_each(std::function<void(NodeBase &)> f) = 0;
    
    virtual std::size_t getAckedLength(IdType id) = 0;
    virtual void setAckedLength(IdType id, std::size_t length) = 0;
    virtual std::size_t getSentLength(IdType id) = 0;
    virtual void setSentLength(IdType id, std::size_t length) = 0;
};

class LocalNodeManager : public CommunicatorBase
{
public:
    using ContainerType = std::unordered_map<IdType,NodeBase*>;
    using const_iterator = ContainerType::const_iterator;
    using iterator       = ContainerType::iterator;

    iterator begin() { return m_nodes.begin(); }
    iterator end() { return m_nodes.end(); }
    const_iterator begin() const { return m_nodes.begin(); }
    const_iterator end() const { return m_nodes.end(); }

    virtual void AddNode(NodeBase & node) final
    {
        node.setId(numNodes());
        m_nodes[node.getId()] = &node;
    }

    virtual std::size_t numNodes() const final { return m_nodes.size(); }

    void send(MessageBase const & msg, NodeBase & node)
    {
        node.send(msg);
    }

    virtual void send(MessageBase const & msg) final
    {
        for_each([this,&msg](NodeBase & n) {
            send(msg, n);
        });
    }

    virtual void send(MessageBase const & msg, IdType id) final
    {
        send(msg, *m_nodes[id]);
    }

    virtual void for_each( std::function<void(NodeBase const&)> f ) const final
    {
        for(auto const & [id, node] : m_nodes)
            f(*node);
    }

    virtual void for_each(std::function<void(NodeBase &)> f) final
    {
        for(auto & [id, node] : m_nodes)
            f(*node);
    }

    virtual std::size_t getAckedLength(IdType id) final { return m_nodes[id]->getAckedLength(); }
    virtual void setAckedLength(IdType id, std::size_t length) final { m_nodes[id]->setAckedLength(length); }

    virtual std::size_t getSentLength(IdType id) final { return m_nodes[id]->getSentLength(); }
    virtual void setSentLength(IdType id, std::size_t length) final { m_nodes[id]->setSentLength(length); }


private:
    ContainerType m_nodes;
};

}