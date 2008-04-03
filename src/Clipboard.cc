// -*- lsst-c++ -*-
/** \file Clipboard.cc
  *
  * \ingroup harness 
  *
  * \brief Container for transport of Image Data between Pipeline Stages 
  *
  *        Clipboard provides a container for carrying image data from one Stage of
  *        the Pipeline to the next.  It wraps a std::map. An image is
  *        accessed via a get() method where a suitable key (e.g., "primary_image")
  *        must be provided.
  *
  * \author Greg Daues, NCSA
  */

#include "lsst/pex/harness/Clipboard.h"

using namespace lsst::pex::harness;

/*****************************************************************************/
/** Initialize the Clipboard structure.
 */
Clipboard::Clipboard() {
}

/** Destroy the Clipboard structure.
 */
Clipboard::~Clipboard() {
}

/** Get the value from the Clipboard that corresponds to the provided key.
 */
void * Clipboard::get(string key) {
    return &dataMap[key];
}

/** Get a vector of the keys of the Clipboard
 */
KeyVector Clipboard::getKeys() {
    KeyVector keyVector;
    map<string, string>::const_iterator iter;

    for (iter = dataMap.begin(); iter != dataMap.end(); ++iter) {
        keyVector.push_back(iter->first); 
    }

    return keyVector;
}

/** Get a vector of the shared keys of the Clipboard.
 *  The "shared" feature is not yet implemented; all keys are returned. 
 */
KeyVector Clipboard::getSharedKeys() {
    KeyVector keyVector;
    map<string, string>::const_iterator iter;     //!< An Iterator over the dataMap

    for (iter = dataMap.begin(); iter != dataMap.end(); ++iter) {
        keyVector.push_back(iter->first); 
    }

    return keyVector;
}

/** Add an entry to the Clipboard, in the form of a key-value pair.
 */
void Clipboard::put(string key, void * p, bool isShared) {
    string* s = (string*)p;
    dataMap.insert( make_pair( key, *s));
}

/** Sets an entry in the Clipboard to be shared. Not yet implemented. 
 */
void Clipboard::setShared(string key, bool isShared) {

}

