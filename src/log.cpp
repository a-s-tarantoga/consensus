// (C) 2023 Martin Huenniger

#include "log.hpp"
#include "message.hpp"

namespace consensus
{

LogEntry::LogEntry(LogEntry const & other) : 
    term(other.term), message(other.message->clone())
{}

LogEntry::LogEntry(TermType term_, MessageBase const & msg_) :
    term(term_), message(msg_.clone())
{}

std::ostream & operator<<(std::ostream & os, LogEntry const & e)
{
    if(e.message == nullptr)
        return os << "Term: " << e.term << ", nullptr";
    return os << "Term: " << e.term << ", " << *e.message;

}

std::ostream & operator<<(std::ostream & os, LogType const & log)
{
    bool first {true};
    for(LogEntry const & entry : log)
    {
        if(first)
        {
            first = false;
        }
        else 
        {
            os << ", ";   
        }
        os << entry;
    }
    return os;
}

}