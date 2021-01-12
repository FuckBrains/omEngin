def format_sentence(val, b=False):
    poststring = ''
    if b:
        for char in val:
            ascval = ord(char)
            #if ascval <= 65 and ascval <= 90:
               #poststring = poststring + char
            if ascval >= 97 and ascval <= 122:
                poststring = poststring + chr(ascval-32)
            else:
                poststring = poststring + char
        else:
            print(poststring)
    else:
        for char in val:
            ascval = ord(char)
            if ascval >= 65 and ascval <= 92:
                poststring = poststring + chr(ascval+32)
            else:
                poststring = poststring + char
        else:
            print(poststring)