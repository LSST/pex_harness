// -*- lsst-c++ -*-
/** \file Queue.cc
  *
  * \ingroup harness
  *
  * \brief   Queue provides the interface for a Stage to access and post image data.
  *
  *          Queue provides the interfaces for a Stage to 1) obtain the top Clipboard
  *          container that will provide it with the next image it should operate on
  *          (via the InputQueue interface) and 2) post a Clipboard container that has
  *          been updated with the image that the present Stage has completed work on
  *          for the next Stage (via the OutputQueue interface)
  *
  * \author  Greg Daues, NCSA
  */

#include "lsst/pex/harness/Queue.h"

using namespace lsst::pex::harness;

/*****************************************************************************/
/** Initialize the Queue structure.
 */
Queue::Queue() {
}

/** Destroy the Queue structure.
 */
Queue::~Queue() {
}

/** Returns the first Clipboard in the vector of datasets in this Queue, 
 *  removing that Clipboard from the vector in the process. 
 *  This is a method of the InputQueue interface.
 */
Clipboard Queue::getNextDataset() {
    Clipboard d1 = clipboardVector.at(0);
    clipboardVector.erase(clipboardVector.begin(),clipboardVector.begin()+1);
    return d1;
}

/** Appends the provided Clipboard to the vector of datasets in this Queue.  
 *  This is a method of the OutputQueue interface.
 */
void Queue::addDataset(Clipboard d) {
    clipboardVector.push_back(d); 
    return;
}

/** Return the number of Clipboards present in the vector of datasets. 
 */
int Queue::size() {
    return clipboardVector.size();
}

