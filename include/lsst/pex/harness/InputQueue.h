// -*- lsst-c++ -*-
/** \file InputQueue.h
  *
  * \ingroup harness
  *
  * \brief   InputQueue provides the interface for a Stage to access image data.
  *
  * \author  Greg Daues, NCSA
  */

#ifndef LSST_DPS_INPUTQUEUE_H
#define LSST_DPS_INPUTQUEUE_H

#include "Clipboard.h"

namespace lsst {

    namespace pex {

    	namespace harness {

/**
  * \brief   InputQueue provides the interface for a Stage to access image data.
  *
  *          inputQueue declares the virtual function  getNextDataset()
  *          to be used for the access of input data by a processing Stage.
  *          As its methods are virtual, it cannot be instantiated.
  *
  */

class InputQueue { 

public:
    InputQueue() {} // constructor

    virtual ~InputQueue() {} // virtual destructor

    virtual Clipboard getNextDataset() = 0; // pure virtual getNextDataset() function

};

	    } // namespace harnes

    } // namespace pex

} // namespace lsst

#endif // LSST_DPS_INPUTQUEUE_H
