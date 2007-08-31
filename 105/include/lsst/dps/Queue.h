// -*- lsst-c++ -*-
/** \file Queue.h
  *
  * \ingroup dps
  *
  * \brief   Queue provides the interface for a Stage to access and post image data.
  *
  * \author  Greg Daues, NCSA
  */

#ifndef LSST_DPS_QUEUE_H
#define LSST_DPS_QUEUE_H

#include "Clipboard.h"
#include "OutputQueue.h"
#include "InputQueue.h"
#include <vector>

using namespace std;

namespace lsst {

    namespace dps {

typedef vector<Clipboard> ClipboardVector;

/**
  * \brief   Queue provides the interface for a Stage to access and post image data.
  *
  *          Queue provides the interfaces for a Stage to 1) obtain the top Clipboard 
  *          container that will provide it with the next image it should operate on 
  *          (via the InputQueue interface) and 2) post a Clipboard container that has 
  *          been updated with the image that the present Stage has completed work on 
  *          for the next Stage (via the OutputQueue interface)
  *
  */

class Queue : public InputQueue, public OutputQueue { 

public:
    Queue(); // constructor

    ~Queue(); //  destructor

    virtual Clipboard getNextDataset();    // virtual getNextDataset() function 

    virtual void addDataset(Clipboard d);  // virtual addDataset(Clipboard d)  

    int size();                             // obtain the size of the vector 

private:
    ClipboardVector clipboardVector;

};

    } // namespace dps

} // namespace lsst

#endif // LSST_DPS_QUEUE_H

