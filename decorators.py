def target_format(fn):
    def fix_target(target):
        if target[0] is not '/':
            target = '/' + target
        if target[-1] is '/':
            target = target[:-1]
        return target
    def wrapper(*args, **kwargs):
        # is it in the kwargs?
        try:
            kwargs["target"] = fix_target(kwargs["target"])
            return fn(args[0], **kwargs)
        except KeyError as ke:
            # no? maybe its in the args
            pass
        try:
            args = (args[0], fix_target(args[1]))
            return fn(*args)
        except IndexError as ie:
            # target not provided
            pass
        try:
            user_dir = args[0].user_dir
            return fn(args[0], user_dir)
        except (AttributeError, IndexError) as e:
            print("neither 'target' nor 'self' argument found")
            raise e
    return wrapper
