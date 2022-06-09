/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef MESSAGE_TYPE_HPP
#define MESSAGE_TYPE_HPP

//! Maximum length of the name of a message
#define MSG_NAMELEN 32

#define MESSAGETYPES							\
	HELPER_MACRO(SYS_FINISH)					\
	HELPER_MACRO(DATA_RAW)						\
	HELPER_MACRO(DMALLOC)						\
	HELPER_MACRO(DFREE)							\
	HELPER_MACRO(DATA_FETCH)					\
	HELPER_MACRO(DATA_SEND)						\
	HELPER_MACRO(TASK_NEW)						\
	HELPER_MACRO(TASK_FINISHED)					\
	HELPER_MACRO(SATISFIABILITY)				\
	HELPER_MACRO(RELEASE_ACCESS)				\
	HELPER_MACRO(RELEASE_ACCESS_AND_FINISH)		\
	HELPER_MACRO(ACCESS_INFO)

typedef enum {
#define HELPER_MACRO(val) val,
	MESSAGETYPES
#undef HELPER_MACRO
	TOTAL_MESSAGE_TYPES
} MessageType;

//! Defined in MessageType.cpp
extern const char MessageTypeStr[TOTAL_MESSAGE_TYPES][MSG_NAMELEN];

#endif /* MESSAGE_TYPE_HPP */
