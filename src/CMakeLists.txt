# (C) 2023 Martin Huenniger

# Add main.cpp file of project root directory as source file
set(SOURCE_FILES 
        log.hpp,    
        log.cpp
        main.cpp, 
    )

# Add executable target with source files listed in SOURCE_FILES variable
add_executable(schedule_planner ${SOURCE_FILES})