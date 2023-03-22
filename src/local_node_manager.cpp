// (C) 2023 Martin Huenniger

#include "local_node_manager.hpp"

#include <iostream>
#include <unordered_map>

namespace consensus
{

void LocalNodeManager::AddNode(NodeBase & node)
{
    node.setId(numNodes());
    m_nodes[node.getId()] = &node;
}

std::size_t LocalNodeManager::numNodes() const 
{ 
    return m_nodes.size(); 
}

void LocalNodeManager::send(MessageBase const & msg, NodeBase & node)
{
    node.send(msg);
}

void LocalNodeManager::send(MessageBase const & msg)
{
    for_each([this,&msg](NodeBase & n) {
        send(msg, n);
    });
}

void LocalNodeManager::send(MessageBase const & msg, IdType id)
{
    send(msg, *(m_nodes[id]));
}

void LocalNodeManager::for_each( std::function<void(NodeBase const&)> f ) const
{
    for(auto const & [id, node] : m_nodes)
        f(*node);
}

void LocalNodeManager::for_each(std::function<void(NodeBase &)> f)
{
    for(auto & [id, node] : m_nodes)
        f(*node);
}

std::size_t LocalNodeManager::getAckedLength(IdType id)
{ 
    return m_nodes[id]->getAckedLength();
}
void LocalNodeManager::setAckedLength(IdType id, std::size_t length)
{ 
    m_nodes[id]->setAckedLength(length); 
}

std::size_t LocalNodeManager::getSentLength(IdType id)
{ 
    return m_nodes[id]->getSentLength(); 
}

void LocalNodeManager::setSentLength(IdType id, std::size_t length)
{ 
    m_nodes[id]->setSentLength(length); 
}

}