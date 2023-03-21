// (C) 2023 Martin Huenniger

#include "log.hpp"
#include "node_manager.hpp"
#include "raft.hpp"

#include <iostream>

int main(int argc, char** argv)
{
    consensus::LocalNodeManager manager;

    consensus::RaftNode node1(manager);
    node1.setAppCallback([](consensus::LogEntry const & entry){ std::cout << "node1: " << entry << std::endl;});
    consensus::RaftNode node2(manager);
    node2.setAppCallback([](consensus::LogEntry const & entry){ std::cout << "node1: " << entry << std::endl;});
}