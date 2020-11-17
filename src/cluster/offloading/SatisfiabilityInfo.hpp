/*
	This file is part of Nanos6 and is licensed under the terms contained in the COPYING file.

	Copyright (C) 2019 Barcelona Supercomputing Center (BSC)
*/

#ifndef SATISFIABILITY_INFO_HPP
#define SATISFIABILITY_INFO_HPP


namespace TaskOffloading {

	//! Type to describe satisfiability info that we communicate between
	//! two cluster nodes
	struct SatisfiabilityInfo {
		//! The region related with the satisfiability info
		DataAccessRegion _region;

		//! node index of the current location
		//! -42 means the directory
		//! -1 means nullptr (only if sending write satisfiability before read satisfiability: very rare)
		int _src;

		//! makes access read satisfied
		bool _readSat;

		//! makes access write satisfied
		bool _writeSat;

		//! hint bit to disable remote propagation (may still not happen even if allowed)
		bool _noRemotePropagation;

		SatisfiabilityInfo(DataAccessRegion region, int src, bool read, bool write, bool noRemotePropagation)
			: _region(region), _src(src), _readSat(read), _writeSat(write), _noRemotePropagation(noRemotePropagation)
		{
			std::cout << "construct SatisfiabilityInfo with nrp = " << noRemotePropagation << "\n";
		}

		bool empty() const
		{
			return !_readSat && !_writeSat && !_noRemotePropagation;
		}

		friend std::ostream& operator<<(std::ostream &o,
				TaskOffloading::SatisfiabilityInfo const &satInfo);
	};

	inline std::ostream& operator<<(
		std::ostream &o,
		TaskOffloading::SatisfiabilityInfo const &satInfo
	) {
		return o << "Satisfiability info for region:" << satInfo._region <<
			" read:" << satInfo._readSat <<
			" write:" << satInfo._writeSat <<
			" location:" << satInfo._src <<
			" no-remote-propagation: " << satInfo._noRemotePropagation;
	}

}


#endif /* SATISFIABILITY_INFO_HPP */
