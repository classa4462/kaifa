#coding: utf-8
'''
Created on 2019年12月3日

@author: sunjie

 Key-Value的本地实现
 
 基于cacheout库： https://github.com/dgilland/cacheout/blob/master/LICENSE.rst
 
         License
        
        The MIT License (MIT)
        
        Copyright (c) 2018, Derrick Gilland
        
        Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), 
        to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
        and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
        
        The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
        
        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
        MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE 
        LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
        CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''
import cacheout

class  Registry(object):
    '''
            键值实现的注册库
        直接使用cacheout来实现
    '''
    def __init__(self, func_before_register=None, func_after_register=None, func_before_unregister=None, func_after_unregister=None):
        super(Registry, self).__init__()
        #创建cacheout.Cache对象存储注册项。maxsize=0禁止淘汰算法，ttl=0禁止超时算法
        self.__items = cacheout.Cache(maxsize=0,ttl=0)
        self.FuncBeforeRegister = func_before_register
        self.FuncAfterRegister = func_after_register
        self.FuncBeforeUnregister = func_before_unregister
        self.FuncAfterUnregister = func_after_unregister
        self.items = self.__items.items
        self.keys = self.__items.keys
        self.values = self.__items.values

    @property
    def AsDict(self):
        return dict(self.__items.copy())
    @AsDict.setter
    def AsDict(self, value):
        self.Clear()
        self.__items.add_many(value)
    
    @property
    def Count(self):
        return self.__items.size()
        
    def Clear(self):
        self.__items.clear()
        
    def Register(self, name, value):
        '添加一个注册项。如果已存在则抛出错误'
        if callable(self.FuncBeforeRegister):
            self.FuncBeforeRegister(self, name, value) 
        self.__items.add(name, value)
        if callable(self.FuncAfterRegister):
            self.FuncAfterRegister(self, name, value) 
    
    def Unregister(self, name):
        '删除一个注册项'
        if callable(self.FuncBeforeUnregister):
            self.FuncBeforeUnregister(self, name) 
        self.__items.delete(name)
        if callable(self.FuncAfterUnregister):
            self.FuncAfterUnregister(self, name) 
    
    def Get(self, name, **kwargs):
        '获取注册项的值'
        if 'default' in kwargs:
            return self.__items.get(name, default=kwargs['default'])
        else:
            if self.Has(name):
                return self.__items.get(name)
            else:
                raise KeyError('"%s" not found' % name)
    
    def _Set(self, name, value):
        self.__items.set(name, value)
    
    def Has(self, name):
        '检查注册项是否存在'
        return self.__items.has(name)
    
    def Names(self):
        '所有已注册的名称'
        return list(self.__items.keys())
    
    def Find(self, wildcard):
        're搜索'
        return self.__items.get_many(wildcard)
    
    def NameOfValue(self, value):
        _r = []
        for _k, _v in self.items():
            if _v == value:
                _r.append(_k)
        return _r
    
    def __str__(self):
        return '<%s Count=%d>' % (self.__class__.__name__, len(self.__items))
    
#     def __repr__(self):
#         return '<%s Count=%d>' % (self.__class__.__name__, len(self.__items))
    
#-----------------------------
    
class Registeable(object):
    '''
            可注册对象基类
        提供了事件接口供继承类使用
    '''
    def __init__(self, owner_data=None):
        super(Registeable, self).__init__()
        self.OwnerData=owner_data
#        self._notify_on_del = Observable()
        
    def __del__(self):
        try:
            self._notify_on_del(self)
        except:
            #此时也许frame已被释放或失效，所以忽略错误
            pass
    
    def _notify_on_del(self, sender):
        pass
    
    def OnBeforeRegister(self, registry):
        pass
    def OnAfterRegister(self, registry):
        pass
    def OnBeforeUnregister(self, registry):
        pass
    def OnAfterUnregister(self, registry):
        pass

class RegisteableRegistry(Registry):
    '''
            可注册对象注册表
        配合Registeable类使用，可以满足业务对象对注册事件的捕捉
    '''
    def __init__(self, owner_data=None):
        super(RegisteableRegistry, self).__init__()
        self.OwnerData=owner_data
        
    @property
    def AsDict(self):
        return super(RegisteableRegistry, self).AsDict()
    @AsDict.setter
    def AsDict(self, value):
        self.Clear()
        for _k, _v in value.items():
            self.Register(_k, _v)

    def _OnItemDel(self, value):
        for _it in self.NameOfValue(value):
            self.__items.delete(_it)
        
    def Clear(self):
        for _name in self.Names():
            self.Unregister(_name)
        
    def IsExists(self, name):
        if isinstance(name, Registeable):
            return name in self.Items.values()
        else:
            return name in self.Items.keys()
        
    def Register(self, name, value):
        '添加一个注册项。如果已存在则抛出错误'
        if self.Has(name):
            raise KeyError('"%s" exists' % name)
        if hasattr(value, 'OnBeforeRegister'):
            value.OnBeforeRegister(self)
        if hasattr(value, '_notify_on_del'):
            value._old_notify_on_del = value._notify_on_del
            value._notify_on_del = self._OnItemDel
        super(RegisteableRegistry, self).Register(name, value)
        if hasattr(value, 'OnAfterRegister'):
            value.OnAfterRegister(self)
    
    def Unregister(self, name):
        '删除一个注册项'
        _value = self.Get(name, default=None)
        if not _value is None:
            if hasattr(_value, 'OnBeforeUnregister'):
                _value.OnBeforeUnregister(self)
            if hasattr(_value, '_notify_on_del'):
                _value._notify_on_del = _value._old_notify_on_del
            super(RegisteableRegistry, self).Unregister(name)
            if hasattr(_value, 'OnAfterUnregister'):
                _value.OnAfterUnregister(self)
        return _value
            
#-------------------------------

# class PriorityQueue(object):
#     '''
#             消息队列
#         初始化时必须传入消息结构定义
#         每个消息具有一个key，可以按key执行Get/Set/Add/Delete
#         支持按结构成员条件搜索多条消息作为返回值
#         支持消息优先级
#         TODO:    支持大尺寸数据的读写
#     '''
#     def __init__(self, define):
#         super(PriorityQueue, self).__init__()
#         
# class MessageQueueInMemory(PriorityQueue):
#     '''
#             内存中的消息队列
#         有尺寸限制
#     '''       
#     def __init__(self, define, max_size=1000):
#         super(MessageQueueInMemory, self).__init__(define)
#         self.__max_size = max_size
#         self.__items = queue.PriorityQueue            
######################
#####################

if __name__ == '__main__':
    pass