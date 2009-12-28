# Tag filesystem utilities

def path2tags(path, target):
    if len(path) == 0:
        return []
    if path == '/':
        return ('dir', ['/'])
    
    tset = path.split('/')
    
    if target == 'dir' or target == 'unsure':
        tset = tset[1:]
    elif target == 'file' and len(tset) == 2:
        tset[0] = '/'

    if tset[-1] == '':
        tset = tset[0:-1]
        return ('dir', tset)

    return (target, tset)

