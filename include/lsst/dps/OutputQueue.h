// -*- lsst-c++ -*-
/** \file OutputQueue.h
  *
  * \ingroup dps
  *
  * \brief   OutputQueue provides the interface for a Stage to post image data. 
  *
  * \author  Greg Daues, NCSA
  */

#ifndef LSST_DPS_OUTPUTQUEUE_H
#define LSST_DPS_OUTPUTQUEUE_H

#include "Clipboard.h"

namespace lsst {

    namespace dps {

/**
  * \brief   OutputQueue provides the interface for a Stage to post image data. 
  *
  *          OutputQueue declares the virtual function   addDataset(Clipboard d)
  *          to be used for the posting of outpout data by a processing Stage.
  *          As its methods are virtual, it cannot be instantiated.
  *
  */

class OutputQueue { 

public:
    OutputQueue() {} // constructor

    virtual ~OutputQueue() {} // virtual destructor

    virtual void addDataset(Clipboard d) = 0; // pure virtual addDataset(Clipboard d) function

};

    } // namespace dps

} // namespace lsst

#endif // LSST_DPS_OUTPUTQUEUE_H

