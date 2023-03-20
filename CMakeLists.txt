# (C) 2023 Martin Huenniger

cmake_minimum_required(VERSION 3.13)  # CMake version check
project(Consensus)                    # Create project "Consensus"
set(CMAKE_CXX_STANDARD 17)            # Enable c++17 standard

add_subdirectory(src)