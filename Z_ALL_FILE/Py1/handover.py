import pandas as pd
import re

#SOCCOM = ["infoadd, 'subject'; 'body'; 'msgcat'; 'concern(optional)'; 'code (optional)'; 'tag (optional)'",
#"handover, No# 'subject'; 'body';'msgcat'; 'concern(optional)'; 'code (optional)'; 'tag (optional)",
#"query, 'text/code/category', date(optional)"]


#def insert_msg(dt, src, subject, body, concern = "", unit = "" code = "", tag = ""):
#    from =

def trcking_format():
    SOCCOM_HELP = """
@infoadd,
subject: mandatory;
body: mandatory;
concern: (optional);
unit: TNR (optional);
code: (optional);
tag: (optional);
date: (optional)
..
            
@infoadd,
subject: DHBDD32 Locked;
body: due to stolen issue, sites locked;
date: 2020-11-20
..
            
@hadover,
1#
subject: tx issue at DHGUL02;
body: due to TNR NCR Activity/other information;
concern: sudipta (optional);
unit: TNR (optional);
code: (optional);
tag: (optional)
..
            
2#
subject: fauction issue of DHGUL02;body: fls is checking, need followup;concern: TBA
.."""
    return SOCCOM_HELP

