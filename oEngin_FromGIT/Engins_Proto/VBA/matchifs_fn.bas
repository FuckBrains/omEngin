Attribute VB_Name = "matchifs_fn"

'''' Function : matchifs() '''''''''''''''''''''''''''''
''''author: ahmedul kabir omi date: 12/11/2020'''''''''
''''author email: omi.kabirr@gmail.com''''''''''''''''''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''calling procedure'''''
''x1 = matchifs("RAW", 2, "CTG_M", 42, "2G", 12, "RB")
'' conditional match for multiple column--- procedure at last [sub match_ifs_calling_ref()] have more example]

'' Araguments takes as ''
'---- shtname = worksheets name
'---- args1_col = colnumber1, args1_str = search_value1 (for column 1)
'---- args2_col = colnumber2, args2_str = search_value2 (for column 2)

'' Returns type = array '' return matching rows based on condition'''-- return [array(0)=0 if no match found]

''' Restriction ''''
'"Multiple column and Multiple values can be passed as arg but same column different value can be matched at single time"

Function matchifs(shtname, ParamArray args()) As Variant

Ln = UBound(args()) + 1
x = Ln Mod 2
If x <> 0 Then
    matchifs = Array("0")
    Exit Function
End If

Set sht = ThisWorkbook.Worksheets(shtname)
Dim values_arr(), rws_arr(), arr_comp() As Variant
j = 0

    For i = 0 To UBound(args())
        ReDim Preserve rws_arr(j)
        ReDim Preserve values_arr(j)
        rval = match_adv(shtname, args(i + 1), args(i))
        rws_arr(j) = Join(rval, ",")
        values_arr(j) = args(i + 1)
        i = i + 1
        j = j + 1
    Next i

    For n = 0 To UBound(rws_arr)
        ReDim Preserve arr_comp(n)
        If n = 0 Then
            dstr = rws_arr(n)
        Else
            tmp = array_comp(dstr, rws_arr(n))
            dstr = tmp
            tmp = ""
        End If
        'MsgBox "Loop: " & n & Chr(10) & "dstr: " & Chr(10) & dstr
    Next n
    
    If InStr(dstr, ",") <> 0 Then
        retarr = Split(dstr, ",")
    Else
        retarr = Array(dstr)
    End If
    
matchifs = retarr

End Function
Function match_adv(shtname, lookvalue, lookcol) As Variant
'Return all macthing row numbers
'return type array/vartype value = 8204 (if ubound array = 0 and value 0 then no match)
'Array for all exact matching element for a single column (for all rows)
Set sht = ThisWorkbook.Worksheets(shtname)
If IsNumeric(lookcol) Then
    mrn = Split(Cells(1, lookcol).Address, "$")(1)
Else
    mrn = lookrn
End If

Set main_rng = sht.Range(mrn & ":" & mrn)
lrw = sht.Cells(Rows.Count, mrn).End(xlUp).Row
Dim allrw() As Variant
Count = 0
frw = 1
For i = frw To lrw
    Set dynrng = sht.Range(mrn & i & ":" & mrn & lrw)
    If Not IsError(Application.Match(lookvalue, dynrng, 0)) Then
        ReDim Preserve allrw(Count)
        MRw_Tmp = Application.Match(lookvalue, dynrng, 0)
        MRw = MRw_Tmp + i - 1
        i = MRw
        sht.Range(mrn & MRw).Interior.ColorIndex = 36
        allrw(Count) = MRw
        Count = Count + 1
    Else
        MRw = 0
    End If
Next i

match_adv = allrw

End Function
Function array_comp(arst1, Optional arst2) As Variant
Dim rarr() As Variant
Count = 0

If IsMissing(arst2) Then
    array_comp = arst1
    Exit Function
Else
    If InStr(arst1, ",") <> 0 Then
        ar1 = Split(arst1, ",")
    Else
        ar1 = Array(arst1)
    End If
    
    If InStr(arst2, ",") <> 0 Then
        ar2 = Split(arst2, ",")
    Else
        ar2 = Array(arst2)
    End If

    For i = 0 To UBound(ar1)
        c1 = ar1(i)
        For n = 0 To UBound(ar2)
            c2 = ar2(n)
            If c1 = c2 Then
                ReDim Preserve rarr(Count)
                rarr(Count) = c1
                Count = Count + 1
            End If
        Next n
    Next i
    
    If Count = 0 Then
        array_comp = 0
    Else
        array_comp = Join(rarr, ",")
    End If
    Exit Function
    
End If


End Function


Sub match_ifs_calling_ref()
'''' This is calling function/test calling for matchifs'''

x = matchifs("RAW", 3, "CRE23", 9, "ERI-HIGH TEMPERATURE") '#test with all matching value
x1 = matchifs("RAW", 2, "CTG_M", 42, "2G", 12, "RB") '#Test for missing in rows
 
'''' Obtained Results ''''
'MsgBox (matchifs("RAW", 3, "CRE23", 9, "ERI-HIGH TEMPERATURE", 4, "CGHLS64"))
'Result1 = get 0 for [CRE24 = "sfdsgsdfgvdfs"]
'Result2 = get 0 for [ERI-HIGH TEMPERATURE = "sfdsgsdfgvdfs"]
'Result3 = get 0 for [ERI-HIGH TEMPERATURE = "sfdsgsdfgvdfs"] and [CGHLS64 = "sfdsgsdfgvdfs"]
    
For n = 0 To UBound(x1)
    If n = 0 Then
        hp = "match rows are" & Chr(10) & x1(n)
    Else
        hp = hp & "," & x1(n)
    End If
Next n
MsgBox hp

End Sub
