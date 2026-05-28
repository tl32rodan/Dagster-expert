#!/bin/csh
# mock tool environment (tcsh-first, per user preference)
# In a real flow `liberate` sources this before characterizing.
setenv LIBERATE_HOME /eda/liberate
setenv PATH /eda/liberate/bin:$PATH
setenv LM_LICENSE_FILE /eda/licenses/mock.lic
