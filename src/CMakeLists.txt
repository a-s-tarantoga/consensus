# (C) 2023 Martin Huenniger

# Add main.cpp file of project root directory as source file
set(SOURCE_FILES 
        log.cpp
        log.hpp    
        main.cpp
        message.cpp
        message.hpp
        node_base.hpp
        node_manager.hpp
        raft.hpp
        types.hpp
    )

# Add executable target with source files listed in SOURCE_FILES variable
add_executable(tester ${SOURCE_FILES})