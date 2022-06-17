/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2018 Barcelona Supercomputing Center (BSC)
*/

#ifndef __GENERIC_FACTORY_HPP__
#define __GENERIC_FACTORY_HPP__

#include <map>
#include "lowlevel/FatalErrorHandler.hpp"

template <typename KEY, typename T, typename... ARGS>
class GenericFactory {
public:
	//! Creates a new object of type T.
	typedef std::function<T(ARGS...)> create_callback;

private:
	//! Maps object type specific keys to callbacks
	std::map<KEY, create_callback> _callbacks;

	GenericFactory() = default;
	~GenericFactory() = default;

	GenericFactory(const GenericFactory&) = delete;
	GenericFactory& operator=(const GenericFactory&) = delete;

public:
	//! Returns true if the registration was successful
	bool emplace(KEY id, create_callback createFn)
	{
		return _callbacks.emplace(id, createFn).second;
	}

	//! Returns true if the id was registered
	bool erase(KEY id)
	{
		return _callbacks.erase(id) == 1;
	}

	T create(KEY id, ARGS... input)
	{
		try {
			return _callbacks.at(id)(input...);
		} catch (const std::out_of_range &oor) {
			FatalErrorHandler::fail(
				"Not registered object: ", typeid(T).name(), " with ID: ", id," in GenericFactory"
			);
		}
		// Should never reach here anyway. This is just to silence the annoying compiler warning
		return T();
	}

	static GenericFactory &getInstance()
	{
		static GenericFactory obj;
		return obj;
	}
};


#endif /* __GENERIC_FACTORY_HPP__ */
