#coding: utf-8
'''
Created on 2019年11月24日

@author: sunjie

序列化实现

根据上一版本的serializer优化

支持任意对象与dict之间的互相转换（反序列化时需先import对应源文件）
支持dict与JSON字符串之间互相转换，支持缩进，支持字符集
支持dict与JSON文件之间互相关换

可选无序列化器的版本信息，
NoType过滤器支持屏蔽对象类型信息（反序列化时无法反射原始类型，需自行解决）
TypeZipper过滤器支持基本类型的数值使用单一字符串表示
DumpedZipper过滤器支持封装压缩头，被压缩部分是完整JSON字符串，头部标志为'ZIP!'，不携带压缩方式
TODO:    可选封装加密头，被加密部分是完整JSON字符串，头部标志为C!，不携带秘钥

MemberZipper         将成员的Dump结果的dict类型数据转换为"类型::值"字符串，未被注册到MemberZipper的类型不会被压缩。默认仅基本类型可被压缩
JsonZipper                将根对象Dump结果转换为JSON字符串时进行压缩
JsonEncrypt              将跟对象Dump结果转换为JSON字符串后进行加密。注意：其与JsonZipper的执行顺序会导致不同结果
上述Filter需在构造Serializer时注册，否则Serialzier的执行结果为可阅读的结构化JSON字符串

目标类可定义__serializable__={}作为序列化参数，此种方式可减少对类实现的侵入（上一版要求目标类的可序列化成员必须是Serializable继承类，使用及调试均带来难度）。
类型注册时以class作为key，注册项中包含名称字符串，注册时进行反向索引，导出数据中使用名称字符串来描述类型。

cacheout支持class作为key，但此时get_many('*')会抛出错误
'''
#---------------------------------
#    类型定义
#--------------------------------
import json
import uuid
import datetime
import decimal
from threading import RLock
import zlib
import base64

from pcs_base.key_value import Registry

_DEFINE_TYPE_NAME = '__type__name__'
# #字符串。不限长度
# class STRING(object):   pass
# #长整型。
# class INTEGER(object):  pass
# #浮点型
# class DOUBLE(object):   pass
# #十进制数值
# class DECIMAL(object):  pass
# #时间戳
# class TIMESTAMP(object):    pass
# #布尔型
# class BOOLEAN(object):  pass
# #数组
# class ARRAY(object):    pass
# #字典
# class DICTIONARY(object):   pass

#----------------------------------
#    错误定义
#----------------------------------

#-----------------------------------
#    序列化注册管理器
#----------------------------------
class SerializerForJSON(object):
    '''
            可序列化类型注册器
        JSON平台兼容性强，对数据类型的兼容性较差，可直接支持的类型有：字符串、整型、浮点型、列表、字典。
        以下常用类型将被转换为字符串：
        * 布尔。转换为'true'和'false'
        * 空值。转换为'null'
        * uuid。
        * 时间。
        * 十进制数值。
        * 元组。转换为数组
        * 集合。转换为？
        * 复数。
    '''
    #TODO:    首先实现最基本的功能：携带类型信息，无压缩，无加密。完成后以过滤器的方式增加特性
    #TODO:    支持complex
    STRING_TYPE_NAME = 'type'
    STRING_VALUE = 'value'
    
    #类型注册表。每一个可序列化的类型均需要注册到此处。基本类型也会注册到此处
    TypesRegistry = Registry()          #key=cls, value=define
    __TypeNames = Registry()        #key=__class__.__name__, value=cls
    FuncBeforeRegisterType = None
    FuncAfterRegisterType = None
    FuncBeforeUnregisterType = None
    FuncAfterUnregisterType = None
    
    def __init__(self, filters=None):
        super(SerializerForJSON, self).__init__()
        self._Filters = filters if isinstance(filters, list) else []
        self.__stack = []
        self.__stack_lock = RLock()
        
    @property
    def Stack(self):
        '当前处理的对象栈。0总是指向当前对象，1是父对象……'
        with self.__stack_lock:
            return self.__stack.copy()
        
    @staticmethod
    def RegisterType(cls, define=None):
        '注册一个可序列化类型。类型定义中包含__serializable_define__结构。基本类型不含此结构，在本类的尾部进行注册'
        #获取序列化描述信息
        if define is None:
            define = getattr(cls, '__serializable_define__', None)
        if define is None:
            raise TypeError('No "__serializable_define__" in class')
        if not SerializerForJSON.FuncBeforeRegisterType is None:
            SerializerForJSON.FuncBeforeRegisterType(cls, define)
        #注册类型
        SerializerForJSON.TypesRegistry.Register(cls, define)
        #添加类型名称索引表
        _type_name = define.get('type_name', cls.__name__)
        SerializerForJSON.__TypeNames._Set(_type_name, cls)
        if not SerializerForJSON.FuncAfterRegisterType is None:
            SerializerForJSON.FuncAfterRegisterType(cls, define)
    @staticmethod
    def UnregisterType(cls):
        '删除一个已注册的类型。'
        if SerializerForJSON.TypesRegistry.Has(cls):
            if not SerializerForJSON.FuncBeforeUnregisterType is None:
                SerializerForJSON.FuncBeforeUnregisterType(cls)
            _type_name = SerializerForJSON.TypesRegistry.Get(cls).get('type_name', cls.__name__)
            SerializerForJSON.TypesRegistry.Unregister(cls)
            SerializerForJSON.__TypeNames.Unregister(_type_name)
            if not SerializerForJSON.FuncAfterUnregisterType is None:
                SerializerForJSON.FuncAfterUnregisterType(cls)
        
    def DumpedToString(self, data, indent=None):
        '将导出的数据转换成为json字符串'
        #TODO:    增加序列化器版本
        _r = json.dumps(data, indent=indent, ensure_ascii=False)                #注意：    这里强制使中文可阅读，尚未知在其他场景会否有问题
        for _filter in self._Filters:
            _dumper  = _filter().GetFunction('dumper')
            if not _dumper is None:
                _r = _dumper(self, _r)
        return _r

    def DumpedFromString(self, text):
        '将json字符串转换成为导出数据'
        _r = text
        for _filter in self._Filters:
            _loader  = _filter().GetFunction('loader')
            if not _loader is None:
                _r = _loader(self, _r)
        return json.loads(_r)

    def DumpedToFile(self, data, filename, indent=None):
        '将导出的数据转换成为json字符串'
        _str = self.DumpedToString(data, indent=indent)
        if isinstance(_str, bytes):
            _f = open(filename, 'wb')
        else:
            _f = open(filename, 'w')
        _f.write(_str)  #.encode() if isinstance(_str, str) else _str)
        _f.flush()
        _f.close()

    def DumpedFromfile(self, filename):
        '将json字符串转换成为导出数据'
        try:
            with open(filename, 'r') as _f:
                _s = _f.read()
                return  self.DumpedFromString(_s)
        except:
            with open(filename, 'rb') as _f:
                _s = _f.read()
                return  self.DumpedFromString(_s)

    @staticmethod
    def GetTypeName(obj):
        '被序列化对象可以自定义自己的类型名称，否则使用Python自动的类型名称'
        getattr(obj, '__serializable_type__', obj.__class__.__name__)
        
    #TODO:    序列化功能应该完全还原对象，所以type_name并无意义
    def Dump(self, obj, type_name=None):
        '将任意类型的obj导出为dict对象。导出的dict可以直接与json相互转换'
        #如果指定了类型名称，则用类型名称推导出类型，否则直接使用对象的类型
#         with self.__stack_lock:
#             self.__stack.insert(0, obj)
        if True:
            if not type_name is None:
                if not SerializerForJSON.__TypeNames.Has(type_name):
                    raise TypeError('unregisteredtype "%s"' % type_name)
                _cls = SerializerForJSON.__TypeNames.Get(type_name)
            else:
                _cls = obj.__class__
                type_name = SerializerForJSON.TypesRegistry.Get(_cls).get('type_name', _cls.__name__)
            #获取类型对应的序列化函数完成转换
            _define = SerializerForJSON.TypesRegistry.Get(_cls)
            _dumper = _define.get('dumper')
            if _dumper is None:
                raise TypeError('function "dumper" not defined')
            _r = {SerializerForJSON.STRING_TYPE_NAME:type_name, 
                  SerializerForJSON.STRING_VALUE:_dumper(self, obj,_define)
                  }
            for _filter in self._Filters:
                _dumper  = _filter().GetFunction('member_dumper')
                if not _dumper is None:
                    _r = _dumper(self, _r)
#            self.__stack.pop(0)
            return _r
        
    #TODO:    序列化功能应该完全还原对象，所以type_name并无意义
    def Load(self, data, type_name=None):
        '将导出的数据载入成为obj'
        if True:
            if data is None:
                raise ValueError('invalid dump data')
            for _filter in self._Filters:
                _loader  = _filter().GetFunction('member_loader')
                if not _loader is None:
                    data = _loader(self, data)
            if type_name is None:
                if isinstance(data, str):
                    x=2
                type_name = data.get(SerializerForJSON.STRING_TYPE_NAME)
            if type_name is None:
                raise ValueError('invalid type_name')
            #未被注册的类型均不可被序列化，直接抛出
            if not SerializerForJSON.__TypeNames.Has(type_name):
                raise TypeError('unregisteredtype "%s"' % type_name)
            #获取类型对应的序列化函数完成转换
            _define = SerializerForJSON.TypesRegistry.Get(SerializerForJSON.__TypeNames.Get(type_name))
            _loader = _define.get('loader')
            if _loader is None:
                raise TypeError('function "loader" not defined')
            _value = data.get(SerializerForJSON.STRING_VALUE)
            _r = _loader(self, _value, _define)
#            self.__stack.pop(0)
            return _r
    
    #类的标准序列化函数。
    #TODO:    序列化功能应该完全还原对象，所以members的数据类型并无意义
    @staticmethod
    def ClassLoader(serializer, data, define):
        '类载入函数。默认序列化规则的类可将此函数作为序列化参数的loader'
        if 'on_before_create' in define:
            define['on_after_create'](serializer, data, define, None)
        _creator = define.get('creator')
        if _creator is None:
            raise TypeError('function "creator" not defined')
        _r = _creator(serializer)
        with serializer.__stack_lock:
            serializer.__stack.insert(0, _r)
            if 'on_before_load' in define:
                define['on_before_load'](serializer, data, define, None)
            _before_class_load = getattr(_r, 'OnBeforeClassLoad', None)
            if not _before_class_load is None:
                _before_class_load(serializer, _r, define, data)
            _members = define.get('members', {})
            for _name, _type in _members.items():
                _value = serializer.Load(data.get(_name), _type)
                setattr(_r, _name, _value)
            _after_class_load = getattr(_r, 'OnAfterClassLoad', None)
            if not _after_class_load is None:
                _after_class_load(serializer, _r, define, data)
            if 'on_after_load' in define:
                define['on_after_load'](serializer, data, define, _r)
            serializer.__stack.pop(0)
        return _r
        
    @staticmethod
    def ClassDumper(serializer, obj, define):
        '类导出函数。默认序列化规则的类可将此函数作为序列化参数的dumper'
        with serializer.__stack_lock:
            serializer.__stack.insert(0, obj)
            _r = {}
            if 'on_before_dump' in define:
                define['on_before_dump'](serializer, obj, define, _r)
            _before_class_dump = getattr(obj, 'OnBeforeClassDump', None)
            if not _before_class_dump is None:
                _before_class_dump(serializer, obj, define, _r)
            _members = define.get('members', {})
            for _name, _type in _members.items():
                _value = serializer.Dump(getattr(obj, _name), _type)
                _r[_name] = _value
            _after_class_dump = getattr(obj, 'OnAfterClassDump', None)
            if not _after_class_dump is None:
                _after_class_dump(serializer, obj, define, _r)
            if 'on_after_dump' in define:
                #用于支持dumped数据压缩，所以修改data
                define['on_after_dump'](serializer, obj, define, _r)
            serializer.__stack.pop(0)
        return _r

#日期时间类型的格式化字符串，%f表示毫秒
STRING_DATETIME_FMT = '%Y-%m-%d %H:%M:%S %f'
#注册基本类型
SerializerForJSON.RegisterType(('').__class__,
             {'creator':lambda ser: '', 'loader':lambda ser, data, define: data, 'dumper':lambda ser, obj, define:obj})
SerializerForJSON.RegisterType((b'').__class__,
             {'creator':lambda ser: b'', 'loader':lambda ser, data, define: base64.b64decode(data).encode('utf8'), 'dumper':lambda ser, obj, define:base64.b64encode(obj).decode('utf8')})
SerializerForJSON.RegisterType((0).__class__,
             {'creator':lambda ser: 0, 'loader':lambda ser, data, define: data, 'dumper':lambda ser, obj, define:obj})
SerializerForJSON.RegisterType((0.0).__class__,
             {'creator':lambda ser: 0.0, 'loader':lambda ser, data, define: data, 'dumper':lambda ser, obj, define:obj})
SerializerForJSON.RegisterType((True).__class__,
             {'creator':lambda ser: False, 'loader':lambda ser, data, define: data, 'dumper':lambda ser, obj, define:obj})
SerializerForJSON.RegisterType((None).__class__,
            {'creator':lambda ser: None, 'loader':lambda ser, data, define: None, 'dumper':lambda ser, obj, define:''})
SerializerForJSON.RegisterType((uuid.uuid4()).__class__,
            {'creator':lambda ser: uuid.uuid4(), 'loader':lambda ser, data, define: uuid.UUID(data), 'dumper':lambda ser, obj, define:str(obj)})
SerializerForJSON.RegisterType((datetime.datetime.min).__class__,
            {'creator':lambda ser: datetime.datetime.min, 'loader':lambda ser, data, define: datetime.datetime.strptime(data, STRING_DATETIME_FMT), 'dumper':lambda ser, obj, define:obj.strftime(STRING_DATETIME_FMT)})
SerializerForJSON.RegisterType(decimal.Decimal(0).__class__,
            {'creator':lambda ser: decimal.Decimal(0), 'loader':lambda ser, data, define: decimal.Decimal(data), 'dumper':lambda ser, obj, define:str(obj)})
SerializerForJSON.RegisterType([].__class__,
            {'creator':lambda ser: [], 'loader':lambda ser, data, define: [ser.Load(x) for x in data], 'dumper':lambda ser, obj, define:[ser.Dump(x) for x in obj]})
SerializerForJSON.RegisterType(().__class__,
            {'creator':lambda ser: (), 'loader':lambda ser, data, define: tuple([ser.Load(x) for x in data]), 'dumper':lambda ser, obj, define:[ser.Dump(x) for x in obj]})
def dict_loader(ser, data, define):
    _r = {}
    for _k, _v in data.items():
        _r[ser.Load(eval(_k))] = ser.Load(_v)
    return _r
def dict_dumper(ser, obj, define):
    _r = {}
    for _k, _v in obj.items():
        _r[repr(ser.Dump(_k))] = ser.Dump(_v)
    return _r
SerializerForJSON.RegisterType({}.__class__,
            {'creator':lambda ser: {}, 'loader':lambda ser, data, define:dict_loader(ser, data, define), 'dumper':lambda ser, obj, define:dict_dumper(ser, obj, define)})

# SerializerForJSON.RegisterType({}.__class__,
#             {'creator':lambda ser: {}, 'loader':lambda ser, data, define:dict(zip([ser.Load(eval(x)) for x in data.keys()], [ser.Load(y) for y in data.values()])), 'dumper':lambda ser, obj, define:dict(zip([repr(ser.Dump(x)) for x in obj.keys()], [ser.Dump(y) for y in obj.values()]))})
SerializerForJSON.RegisterType(set().__class__,
            {'creator':lambda ser: set(), 'loader':lambda ser, data, define:set([ser.Load(x) for x in data]), 'dumper':lambda ser, obj, define:[ser.Dump(x) for x in obj]})
#TODO:    增加更多类型

class NoType(object):
    '序列化过滤器——去除类型'
    def __init__(self):
        super(NoType, self).__init__()

    def GetFunction(self, name):
        if name == 'member_dumper':    return lambda ser, dump: dump.get(ser.STRING_VALUE)
        else:   return None

class TypeZipper(object):
    '序列化过滤器——值压缩'
    TypesRegistry = Registry()          #key=cls
    def __init__(self):
        super(TypeZipper, self).__init__()

    @staticmethod
    def RegisterType(type_name, define=None):
        TypeZipper.TypesRegistry.Register(type_name, define)
    @staticmethod
    def UnregisterType(type_name):
        TypeZipper.TypesRegistry.Remove(type_name)

    def GetFunction(self, name):
        if name == 'member_loader':    return self.OnLoadMember
        elif name == 'member_dumper':    return self.OnDumpMember
        else:   return None
        
    def TypeNameByZipped(self, s):
        for _name in self.TypesRegistry.Names():
            _define = self.TypesRegistry.Get(_name)
            if _define.get('type_name') == s:
                return _name
        return None
    
    def OnDumpMember(self, serializer, dump):
        _type_name = dump.get(serializer.STRING_TYPE_NAME)
        if _type_name  is None or not self.TypesRegistry.Has(_type_name):
            return dump
        _value = dump.get(serializer.STRING_VALUE)
        _define = self.TypesRegistry.Get(_type_name)
        return '%s::%s' % (_define.get('type_name', _type_name), repr(_define.get('dumper')(_value)))
    
    def OnLoadMember(self, serializer, dump):
        if not isinstance(dump, str):
            return dump
        _index = dump.find('::')
        _type_name = self.TypeNameByZipped(dump[:_index])
        if _type_name  is None or not self.TypesRegistry.Has(_type_name):
            return dump
        _value = dump[_index+2:]
        _define = self.TypesRegistry.Get(_type_name)
        return {serializer.STRING_TYPE_NAME:_type_name, serializer.STRING_VALUE:_define.get('loader')(eval(_value))}
    
TypeZipper.RegisterType(('').__class__.__name__, {'type_name':'S', 'dumper':lambda value:value, 'loader':lambda value:value})
TypeZipper.RegisterType((0).__class__.__name__, {'type_name':'I', 'dumper':lambda value:value, 'loader':lambda value:value})
TypeZipper.RegisterType((0.0).__class__.__name__, {'type_name':'F', 'dumper':lambda value:value, 'loader':lambda value:value})
TypeZipper.RegisterType((True).__class__.__name__, {'type_name':'B', 'dumper':lambda value:'T' if value else 'F', 'loader':lambda value:True if value=='T' else False})
TypeZipper.RegisterType((None).__class__.__name__, {'type_name':'N', 'dumper':lambda value:'', 'loader':lambda value:''})
TypeZipper.RegisterType((uuid.uuid4()).__class__.__name__, {'type_name':'U', 'dumper':lambda value:value, 'loader':lambda value:value})
TypeZipper.RegisterType((decimal.Decimal(0)).__class__.__name__, {'type_name':'D', 'dumper':lambda value:value, 'loader':lambda value:value})
TypeZipper.RegisterType((datetime.datetime.min).__class__.__name__, {'type_name':'T', 'dumper':lambda value:value, 'loader':lambda value:value})
TypeZipper.RegisterType(([]).__class__.__name__, {'type_name':'LI', 'dumper':lambda value:value, 'loader':lambda value:value})
TypeZipper.RegisterType((()).__class__.__name__, {'type_name':'TU', 'dumper':lambda value:value, 'loader':lambda value:value})
TypeZipper.RegisterType(({}).__class__.__name__, {'type_name':'DI', 'dumper':lambda value:value, 'loader':lambda value:value})
TypeZipper.RegisterType((set()).__class__.__name__, {'type_name':'SE', 'dumper':lambda value:value, 'loader':lambda value:value})


class DumpedZipper(object):
    '序列化过滤器——压缩/解压缩'
    def __init__(self):
        super(DumpedZipper, self).__init__()

    def GetFunction(self, name):
        if name == 'dumper':    
            return self.OnDump
        elif name == 'loader':    
            return self.OnLoad
        else:   return None

    def OnDump(self, ser, data):
        _r = b'ZIP!' + zlib.compress(data.encode())
        return _r

    def OnLoad(self, ser, data):
        if data[:4] == b'ZIP!':
            return zlib.decompress(data[4:]).decode()
        return data
    
##############################
##############################

def test_SerializerForJSON():
#     print('已注册类型:')
#     for _type_name in SerializerForJSON.TypesRegistry.Names():
#         print('    ', _type_name.__name__, SerializerForJSON.TypesRegistry.Get(_type_name))

    _ser1 = SerializerForJSON()
    _ser2 = SerializerForJSON()
    _ser3 = SerializerForJSON(filters=[TypeZipper])
    _ser4 = SerializerForJSON(filters=[NoType])
    
    #测试基本类型的序列化
    for _obj in ['abc', 123, 456.789, True, None, uuid.uuid4(), decimal.Decimal('111.22'), datetime.datetime.now(),
                 [1, 'abc', datetime.datetime.now()], (456.789, True, None), {'a':'b', 1:2,  uuid.uuid4():datetime.datetime.now()},
                 {1,2,3,'a', None, uuid.uuid4()}]:
        _dumped = _ser1.Dump(_obj)
        print('dumped:', _dumped, '    JSON"%s"' % _ser1.DumpedToString(_dumped), '    ZIPPED"%s"'%_ser1.DumpedToString(_ser3.Dump(_obj)), '    NO_TYPE"%s"'%_ser1.DumpedToString(_ser4.Dump(_obj)))
        _loaded= _ser2.Load(_dumped)
        print('loaded:', _loaded.__class__.__name__, _loaded)
    
    print('-'*10, '用户类序列化')
    #测试类实例的序列化
    class _Class(object):
        __serializable_define__={'creator': lambda ser:_Class(None, None), 
                                            'loader':lambda ser, data, define:SerializerForJSON.ClassLoader(ser, data, define), 
                                            'dumper':lambda ser, obj, define:SerializerForJSON.ClassDumper(ser, obj, define),
                                            'members':{'X':None, 'Y':None, 'A':None, 'B':None, 'BB':('').__class__.__name__},
                                            'on_after_load': lambda ser, data, define, obj: print('已载入:', str(obj))
                                            }
        AA = 'abc'
        def __init__(self, x, y):
            super(_Class, self).__init__()
            self.X = x
            self.Y = y
            self.A = [self.X, self.Y, 1]
            self.B = {'a': lambda :1}
            
        @property
        def BB(self):
            return self.AA
        @BB.setter
        def BB(self, value):
            self.AA = value
            
        def __str__(self, *args, **kwargs):
            return '<%s X=%s, Y=%s, A=%s, B=%s, AA=%s, BB=%s>' % (self.__class__.__name__, repr(self.X), repr(self.Y), repr(self.A), repr(self.B), repr(self.AA), repr(self.BB))
        
    _obj = _Class('px', 'py')
    _obj.BB = 3
    _obj.X = 'x'
    _obj.Y = 'y'
    _obj.B = 'bbbb'
    print('obj_old:    ',str(_obj))
    
    SerializerForJSON.FuncAfterRegisterType = lambda cls, define: print('类型注册：', cls)
    SerializerForJSON.RegisterType(_Class)
#     SerializerForJSON.RegisterType(_Class, {'creator': lambda ser:_Class(None, None), 
#                                             'loader':lambda ser, data, define:SerializerForJSON.ClassLoader(ser, data, define), 
#                                             'dumper':lambda ser, obj, define:SerializerForJSON.ClassDumper(ser, obj, define),
#                                             'members':{'X':None, 'Y':None, 'A':None, 'B':None, 'BB':None},
#                                             })
#     print('已注册类型:')
#     for _type_name in SerializerForJSON.TypesRegistry.Names():
#         print('    ', _type_name.__name__, SerializerForJSON.TypesRegistry.Get(_type_name))
    
    _dumped = _ser1.Dump(_obj)
    print('dumped:', _dumped, '\n    JSON"%s"' % _ser1.DumpedToString(_ser1.Dump(_obj)), '\n    ZIPPED"%s"'%_ser1.DumpedToString(_ser3.Dump(_obj)), '\n    NO_TYPE"%s"'%_ser1.DumpedToString(_ser4.Dump(_obj)))
    _obj_new = _ser2.Load(_dumped)
    print('obj_new:    ', str(_obj_new))

if __name__ == '__main__':
    test_SerializerForJSON()
