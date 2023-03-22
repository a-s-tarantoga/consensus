# (C) 2023 Martin Huenniger

# Add main.cpp file of project root directory as source file
set(SOURCE_FILES 
        communicator_base.hpp    
        local_node_manager.cpp
        local_node_manager.hpp
        log.cpp
        log.hpp    
        main.cpp
        message.cpp
        message.hpp
        node_base.hpp
        raft.cpp
        raft.hpp
        types.hpp
    )

# Add executable target with source files listed in SOURCE_FILES variable
add_executable(tester ${SOURCE_FILES})