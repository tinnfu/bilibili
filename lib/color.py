class color(object):
    BLACK = 0
    RED = 1
    GREEN = 2
    YELLOW = 3
    BLUE = 4
    PURPLE = 5
    DEEP_GREEN = 6
    WHITE = 7

    @staticmethod
    def dye(msg, front_color, back_color = -1):
        return '\033[%s;%sm%s\033[0m' % ([back_color + 40, ''][back_color == -1],
                                        front_color + 30, msg)
