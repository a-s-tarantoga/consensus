// (C) 2023 Martin Huenniger

#include "message.hpp"

namespace consensus
{

std::ostream & operator<<(std::ostream & os, MessageBase const & m)
{
    return os << m.write();
} 

}