#coding: utf-8
'''
Created on 2020年7月28日

@author: sunjie
'''
import logging
import json
from threading import RLock
import sys
import inspect
import urllib

# class withLoggerName(object):
#     @property
#     def LoggerName(self):
#         return getattr(self, '_logger_name', self.__class__.__name__)
#     @LoggerName.setter
#     def LoggerName(self, value):
#         if value != self.LoggerName:
#             self._logger_name = value
# 
# class withEnabled(object):
#     '''
#             属性-Enabled
#     '''
#     @property
#     def Enabled(self):
#         return getattr(self, '_enabled', False)
#     @Enabled.setter
#     def Enabled(self, value):
#         if value != self.Enabled:
#             _func_on_enabled = getattr(self, 'OnEnabled')
#             self._enabled = _func_on_enabled(value) if callable(_func_on_enabled) else value

#--------------------
class NotFoundError(Exception):
    pass

class DuplicateError(Exception):
    pass

class DisabledError(Exception):
    pass

class RpcError(Exception):
    pass

class mixinCommon(object):
    '''
            作为基类使用
    '''
    #默认的Enabled属性值，使实例被创建后Enabled默认为True
    DEFAULT_ENABLED = True
    #是否在日志中包含详细的调试信息
    LOGGING_WITH_DEBUG = True
    
    @property
    def PROP_LOCK(self):
        '属性被读写时必须获取首先获取此线程锁'
        _r = getattr(self, '___property_lock', None)
        if _r is None:
            _r = RLock()
            setattr(self, '___property_lock', _r)
        return _r
        
    @property
    def LoggerName(self):
        with self.PROP_LOCK:
            return getattr(self, '___logger_name', self.__class__.__name__)
    @LoggerName.setter
    def LoggerName(self, value):
        if value != self.LoggerName:
            with self.PROP_LOCK:
                setattr(self, '___logger_name', str(value))
                self.DoNotifyPropertyChanged('LoggerName')
    
    #Enabled属性用于决定组件是否被允许完成业务逻辑，而不应该将设置Enabled作为业务逻辑的一部分，即使OnPropertyChanged事件中可以检测到Enabled的变化
    @property
    def Enabled(self):
        with self.PROP_LOCK:
            return getattr(self, '___enabled', self.DEFAULT_ENABLED)
    @Enabled.setter
    def Enabled(self, value):
        if value != self.Enabled:
            with self.PROP_LOCK:
                setattr(self, '___enabled', value)
                self.DoNotifyPropertyChanged('Enabled')
    
    #EOwnerData属性用于携带一部分业务中的动态数据，通常在回调函数中会将调用者实例作为首参数，通过调用者实例可以得到OwnerData
    @property
    def OwnerData(self):
        with self.PROP_LOCK:
            return getattr(self, '___owner_data', None)
    @OwnerData.setter
    def OwnerData(self, value):
        if value != self.OwnerData:
            with self.PROP_LOCK:
                setattr(self, '___owner_data', value)
                self.DoNotifyPropertyChanged('OwnerData')

    @property
    def NotifyPropertyChanged(self):
        with self.PROP_LOCK:
            return getattr(self, '___notify_property_changed', None)
    @NotifyPropertyChanged.setter
    def NotifyPropertyChanged(self, value):
        if value != self.NotifyPropertyChanged:
            with self.PROP_LOCK:
                setattr(self, '___notify_property_changed', value)
                #这里没有调用DoNotifyPropertyChanged
         
    def DoNotifyPropertyChanged(self, prop_name):
        if callable(self.NotifyPropertyChanged):
            self.NotifyPropertyChanged(sender=self, name=prop_name)

    def LOG_MESSAGE(self, message, full_debug=False):
        if not self.LOGGING_WITH_DEBUG or not full_debug:
            return message
        else:
            _frame = inspect.currentframe()
            _f_code = _frame.f_back.f_code
            _args = inspect.getargvalues(_frame)
            return '{message}\n\t{class_name}.{function_name}({args})\tFile "{filename}" line {lineno}'.format(message=message, filename=_f_code.co_filename, lineno=_f_code.co_firstlineno, class_name=self.__class__.__name__, function_name=_f_code.co_name, args=[_args.locals[x] for x in _args.args])

            
class mixinExecuteable(object):
    '''
            使继承类具有Execute函数。继承类需要实现DoExecute来执行实际的功能
    '''
    #类的Execute函数被调用时触发。回调时的参数可参见Execute函数的实现
    @property
    def NotifyBeforeExecute(self):
        with self.PROP_LOCK:
            return getattr(self, '___notify_before_execute', None)
    @NotifyBeforeExecute.setter
    def NotifyBeforeExecute(self, value):
        if value != self.NotifyBeforeExecute:
            with self.PROP_LOCK:
                setattr(self, '___notify_before_execute', value)
                self.DoNotifyPropertyChanged('NotifyBeforeExecute')

    @property
    def NotifyExecuteSuccess(self):
        with self.PROP_LOCK:
            return getattr(self, '___notify_execute_success', None)
    @NotifyExecuteSuccess.setter
    def NotifyExecuteSuccess(self, value):
        if value != self.NotifyExecuteSuccess:
            with self.PROP_LOCK:
                setattr(self, '___notify_execute_success', value)
                self.DoNotifyPropertyChanged('NotifyExecuteSuccess')

    @property
    def NotifyExecuteError(self):
        with self.PROP_LOCK:
            return getattr(self, '___notify_execute_error', None)
    @NotifyExecuteError.setter
    def NotifyExecuteError(self, value):
        if value != self.NotifyExecuteError:
            with self.PROP_LOCK:
                setattr(self, '___notify_execute_error', value)
                self.DoNotifyPropertyChanged('NotifyExecuteError')

    @property
    def NotifyAfterExecute(self):
        with self.PROP_LOCK:
            return getattr(self, '___notify_after_execute', None)
    @NotifyAfterExecute.setter
    def NotifyAfterExecute(self, value):
        if value != self.NotifyAfterExecute:
            with self.PROP_LOCK:
                setattr(self, '___notify_after_execute', value)
                self.DoNotifyPropertyChanged('NotifyAfterExecute')

    def Execute(self, *args, **kwargs):
        try:
            if callable(self.NotifyBeforeExecute):
                self.NotifyBeforeExecute(sender=self, args=args, kwargs=kwargs)
            _r = self.DoExecute(*args, **kwargs)
            if callable(self.NotifyExecuteSuccess):
                self.NotifyExecuteSuccess(sender=self, args=args, kwargs=kwargs)
            return _r
        except Exception as _e:
            _no_raise = False
            if callable(self.NotifyExecuteError):
                _no_raise = self.NotifyExecuteError(sender=self, args=args, kwargs=kwargs, error=_e)
            if _no_raise != True:
                raise
        finally:
            if callable(self.NotifyAfterExecute):
                self.NotifyAfterExecute(sender=self, args=args, kwargs=kwargs)
                
    def __call__(self, *args, **kwargs):
        return self.Execute(*args, **kwargs)

#装饰器
def TRY_CATCH_FINALLY(owner_data=None, no_raise=False, on_call=None, on_success=None, on_error=None, on_finally=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if callable(on_call):
                on_call(owner_data)
            try:
                _r = func(*args, **kwargs)
                if callable(on_success):
                    on_success(owner_data)
                return _r
            except Exception as _e:
                if callable(on_error):
                    on_error(owner_data)
                if not no_raise:
                    raise
            finally:
                if callable(on_finally):
                    on_finally(owner_data)
        return wrapper
    return decorator
            
def API_RESULT(serializer=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                _r = func(*args, **kwargs)
                #确保_r中的数据可以被json序列化
                if not serializer is None:
                    _r = serializer.Dump(_r)
                return dict(result='success', value=_r)
            except Exception as _e:
                return dict(result='<{e_cls}>: {e_msg}'.format(e_cls=_e.__class__.__name__, e_msg=str(_e)))
                raise
        return wrapper
    return decorator
                
def DEFAULT_ERROR_MESSAGE():
    return '{exception_type}("{exception_message}"), File "{filename}", line {line_no}'.format(
        filename=__file__, line_no=sys._getframe().f_lineno, 
        exception_type=sys.exc_info()[0].__name__, 
        exception_message=sys.exc_info()[1])

    
def VALUE(value):
    '''
            获取变量的值。
        在组件式的结构中，变量可能会使用Refrence对象来实现路径引用，以满足运行时的组件动态装配，此时直接访问变量得到的是Refrence对象，而不是真实的值，所以需要遍历的方式得到最终值返回
    '''
    #TODO:    返回值应该是循环遍历最终得到的值
    return value() if callable(value) else value

def URL2DICT(url):
    '''
            用于支持将各种参数组合至URL中。
        此函数将URL解析至dict变量中
    '''
    _parsed = urllib.parse.urlparse(url, allow_fragments=False)
    _r = dict(scheme=_parsed.scheme, netloc=_parsed.netloc, path=_parsed.path)
    _p = {}
    if _parsed.query != '':
        for _k, _v in urllib.parse.parse_qs(_parsed.query, True, True).items():
            if len(_v) == 0:
                _p[_k] = None
            elif len(_v) == 1:
                _p[_k] = _v[0]
            else:
                _p[_k] = _v
    _r['params'] = _p
    return _r


#--------------------
#TODO：    暂时放在这里

class LoggingByRabbitMQ(object):
    '经过简单检测，每秒可处理1800+个'
    class Handler(logging.Handler):
        def __init__(self, publisher):
            super().__init__()
            self.Publisher = publisher
            self.Fields = None
            
        def emit(self, record):
            _fields = self.Fields if not self.Fields is None else [x for  x in record.__dict__.keys()]
            _pack = {}
            for _name in _fields:
                if hasattr(record, _name):
                    _pack[_name] = str(getattr(record, _name, 'NULL'))
            #格式化后的字符串作为message存储
            _pack['message'] = str(self.format(record))
            _pack['asctime'] = logging.Formatter().formatTime(record)
            self.Publisher.Execute(json.dumps(_pack))
            
    @staticmethod        
    def RecordMessage(data):
        '返回格式化后的字符串。data中也包含了其他信息：name, msg, args, levelname, levelno, pathname, filename, module, exc_info, exc_text, stack_info, lineno, funcName, created, msecs, relativeCreated, thread, threadName, processName, process, message'
        return data.get('message')
    
############################
###########################

@TRY_CATCH_FINALLY(on_call=lambda od:print('Before call function_1()'))
def function_1():
    return 'function_1'
@TRY_CATCH_FINALLY()
def function_2(a, b):
    return 'function_2({a}, {b})'.format(a=a, b=b)
@TRY_CATCH_FINALLY(on_finally=lambda od:print('finally@function_3'))
def function_3(*args, **kwargs):
    return 'function_3({args}, {kwargs})'.format(args=args, kwargs=kwargs)
@TRY_CATCH_FINALLY(on_error=lambda od:print('-'*10, str(sys.exc_info())))
def function_4(*args, **kwargs):
    raise NotImplementedError('测试')

class A(object):
    @TRY_CATCH_FINALLY(on_call=lambda od:print('Before call function_5()'))
    def function_5(self):
        return 'function_5'
    @TRY_CATCH_FINALLY()
    def function_6(self, a, b):
        return 'function_6({a}, {b})'.format(a=a, b=b)
    @TRY_CATCH_FINALLY(on_finally=lambda od:print('finally@function_7'))
    def function_7(self, *args, **kwargs):
        return 'function_7({args}, {kwargs})'.format(args=args, kwargs=kwargs)
    @TRY_CATCH_FINALLY(on_error=lambda od:print('-'*10, str(sys.exc_info())), on_finally=lambda od:print('finally@function_8'))
    def function_8(self, *args, **kwargs):
        raise NotImplementedError
    

def test_TRY_CATCH_FINALLY():
    @TRY_CATCH_FINALLY(on_call=lambda od:print('Before call function_5()'))
    def function_5():
        return 'function_5'
    @TRY_CATCH_FINALLY()
    def function_6(a, b):
        return 'function_6({a}, {b})'.format(a=a, b=b)
    @TRY_CATCH_FINALLY(on_finally=lambda od:print('finally@function_7'))
    def function_7(*args, **kwargs):
        return 'function_7({args}, {kwargs})'.format(args=args, kwargs=kwargs)
    @TRY_CATCH_FINALLY(on_error=lambda od:print('-'*10, str(sys.exc_info())), on_finally=lambda od:print('finally@function_8'))
    def function_8(*args, **kwargs):
        raise NotImplementedError
    
#     print(function_1())
#     print(function_2(1,2))
#     print(function_3(a=1,b=2))
# #    print(function_4(a=1,b=2))
# 
#     print(function_5())
#     print(function_6(1,2))
#     print(function_7(a=1,b=2))
# #    print(function_8(a=1,b=2))
    
    _a = A()
    print(_a.function_5())
    print(_a.function_6(1,2))
    print(_a.function_7(a=1,b=2))
    print(_a.function_8(a=1,b=2))

if __name__ == '__main__':
    test_TRY_CATCH_FINALLY()