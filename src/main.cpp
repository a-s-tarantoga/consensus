// (C) 2023 Martin Huenniger

#include "log.hpp"
#include "message.hpp"
#include "node_base.hpp"
#include "local_node_manager.hpp"
#include "raft.hpp"

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

void wait_and_count(std::size_t t)
{
    std::size_t count {0};
    while(count < t)
    {
        std::cout << "\rWaiting " << ++count << std::flush;  
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

int main(int argc, char** argv)
{
    consensus::LocalNodeManager manager;

    std::vector<std::unique_ptr<consensus::NodeBase>> nodes;
    for(std::size_t i{0}; i<10; ++i)
    {
        nodes.emplace_back(std::make_unique<consensus::RaftNode>(manager));
        nodes.back()->setAppCallback([i=i](consensus::LogEntry const & entry){ std::cout << "node" << i << ": " << entry << std::endl;});
    }
    for(auto & node : nodes)
        node->start();
    wait_and_count(10);
    for(auto & node : nodes)
    {
        node->broadcast(consensus::StringMessage("Message from node " + std::to_string(node->getId())));
        //wait_and_count(1);
    }
    wait_and_count(1);
    for(auto & node : nodes)
        node->stop();
    std::cout << std::endl;
    for(auto & node : nodes)
        std::cout << *node << std::endl;
}