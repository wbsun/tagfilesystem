# Tag filesystem utilities

def path2tags(path, target):
    if len(path) == 0:
        return []
    tset = path.split('/')
    if tset[0] == '':
        if target == 'dir' || target == 'unsure':
            tset = tset[1:]
        elif target == 'file':
            tset[0] = '/'
        else: 
            rasie Exception('Invalid target parameter from path2tags: target'
                            +' = '+target)
    if tset[-1] == '':
        tset = tset
        return ('dir', tset)

    return (target, tset)

