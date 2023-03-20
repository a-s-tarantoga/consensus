// (C) 2023 Martin Huenniger

#pragma once

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

}