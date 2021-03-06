# Если у вас питончик вдруг падает с ошибкой сегментирования
# На самом деле я в итоге откатился с 3.7 на 3.6
# Но это может помочь с отладкой C-extensions

import sys

def trace_calls(frame, event, arg):
    if event != 'call':
        return
    co = frame.f_code
    func_name = co.co_name
    if func_name == 'write':
        # Ignore write() calls from print statements
        return
    func_line_no = frame.f_lineno
    func_filename = co.co_filename
    caller = frame.f_back
    caller_line_no = caller.f_lineno
    caller_filename = caller.f_code.co_filename
    print ('Call to %s on line %s of %s from line %s of %s' % \
        (func_name, func_line_no, func_filename,
         caller_line_no, caller_filename))
    return

sys.settrace(trace_calls)

# и дальше выполняем код

# потом запускаем отладчик
# gdb --args python gp_sheremetyevo_preprocessin^Cof_new_data.py
# в нем пишем run
# и наслаждаемся
# если нужен просто бектрейс, то эта функция нафиг не нужна, просто запускаем отладчик
# пишем run и когда программа упадет пишем:
# backtrace
# всё