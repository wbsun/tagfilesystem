# Tag filesystem utilities

def path2tags(path, target):
    if len(path) == 0:
        return (target, [])
    if path == '/':
        return ('dir', ['/'])
    
    tset = path.split('/')
    
    if target == 'dir' or target == 'unsure':
        del tset[0]
    elif target == 'file' and len(tset) == 2:
        tset[0] = '/'

    if tset[0] == '':
        del tset[0]
        
    if len(tset) > 0 and tset[-1] == '':
        del tset[-1]
        return ('dir', tset)

    return (target, tset)

def files2file(fs):
    if fs[0] != 'files':
        raise Exception('invalid arguments, a files-result is expected.')
    for f in fs[1]:
        if len(f) == 1:
            fs = ('file', f)
            return fs
    return None