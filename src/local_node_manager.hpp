// (C) 2023 Martin Huenniger

#pragma once

#include "node_base.hpp"
#include "communicator_base.hpp"
#include "types.hpp"

#include <functional>
#include <iostream>
#include <unordered_map>

namespace consensus
{

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

    void AddNode(NodeBase & node) final;

    std::size_t numNodes() const final;

    void send(MessageBase const & msg, NodeBase & node);

    void send(MessageBase const & msg) final;

    void send(MessageBase const & msg, IdType id) final;

    void for_each( std::function<void(NodeBase const&)> f ) const final;

    void for_each(std::function<void(NodeBase &)> f) final;

private:
    ContainerType m_nodes;
};

}