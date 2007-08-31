// -*- lsst-c++ -*-
/** \file Clipboard.h
  *
  * \ingroup dps
  *
  * \brief Container for transport of Image Data between Pipeline Stages
  *
  * \author Greg Daues, NCSA
  */

#ifndef LSST_DPS_CLIPBOARD_H
#define LSST_DPS_CLIPBOARD_H

#include <string>
#include <iostream>
#include <map>
#include <vector>

using namespace std;

namespace lsst {

    namespace dps {

typedef vector<string> KeyVector;

/** 
  * \brief Container for transport of Image Data between Pipeline Stages
  *
  *        Clipboard provides a container for carrying image data from one Stage of
  *        the Pipeline to the next.  An image is accessed via a get() method where 
  *        a suitable key (e.g., "primary_image") must be provided.
  *
  * \see Queue class for the use of Clipboard as an image data set container.
  */

class Clipboard { 

public:
    Clipboard(); // constructor

    ~Clipboard(); // virtual destructor

    void * get(string key);

    KeyVector getKeys();

    KeyVector getSharedKeys();

    void put(string key, void * p, bool isShared);

    void setShared(string key, bool isShared); 

private:
    map<string, string> dataMap;

};

    } // namespace dps

} // namespace lsst

#endif // LSST_DPS_CLIPBOARD_H

