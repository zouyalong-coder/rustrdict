use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::hash::{Hash, Hasher};


struct DictEntry<K, V>
    where K: Eq + Default + Debug,
    V: Debug {
    key: K,
    value: Option<V>,
    /// 链表，存储 next 的指针
    next: Option<*mut DictEntry<K, V>>,
}

impl<K, V> Drop for DictEntry<K, V> 
    where K: Eq + Default + Debug,
    V: Debug {
    fn drop(&mut self) {
        // println!("DictEntry drop: key={:?} value={:?} next={:?}", &self.key, &self.value, &self.next);
        if let Some(next) = self.next.take() {
            let _ = unsafe{Box::from_raw(next)};
        }
        // println!("DictEntry real drop: key={:?} value={:?}", &self.key, &self.value)
    }
}

impl <K, V> Debug for DictEntry<K, V>
    where K: Eq + Default + Debug,
    V: Debug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.next.is_some() {
            return f.write_fmt(format_args!("{:?}:{:?} -> {:?}", &self.key, &self.value, unsafe{&mut *self.next.unwrap()}))
        } else {
            return f.write_fmt(format_args!("{:?}:{:?}", &self.key, &self.value))
        }
        // f.debug_struct("DictEntry").field("key", &self.key).field("value", &self.value).field("next", &self.next).finish()
    }
}

impl <K, V> DictEntry<K, V>
    where K: Eq + Default + Debug,
    V: Debug {

    fn empty() -> Self {
        Self { key: Default::default(), value: Default::default(), next: None }
    }

    pub fn find_mut(&mut self, k: &K) -> Option<&mut DictEntry<K, V>> {
        if *k == self.key {
            return Some(self as &mut DictEntry<K, V>);
        }
        if let Some(next) = self.next {
            let next = unsafe{&mut *next};
            next.find_mut(k)
        } else {
            None
        }
    }

    fn remove_tail(&mut self, k: &K) -> Option<DictEntry<K, V>> {
        if let Some(next) = self.next {
            let next = unsafe{&mut *(next as *mut DictEntry<K, V>)}; 
            if next.key == *k {
                self.next = next.next.take();
                let res = Some(std::mem::replace(next, Self::empty()));
                let _ = unsafe{ Box::from_raw(next)};
                res
            } else {
                next.remove_tail(k)
            }
        } else {
            None
        }
    }

    fn insert(&mut self, mut next: DictEntry<K, V>) {
        next.next = self.next;
        let next_ptr = Box::into_raw(Box::new(next));
        self.next = Some(next_ptr);
    }

    fn take_key(&mut self) -> K {
        std::mem::replace(&mut self.key, Default::default())
    }

    fn take_value(&mut self) -> Option<V> {
        self.value.take()
    }

    fn get_value(&self) -> Option<&V> {
        self.value.as_ref()
    }

    fn replace_value(&mut self, v: V) -> Option<V> {
        // let ori = self.value.take();
        // self.value = Some(v);
        // ori
        // std::mem::replace(&mut self.value, v)
        self.value.replace(v)
    }

    fn take_next(&mut self) -> Option<DictEntry<K, V>> {
        if let Some(n) = self.next.take() {
            let ptr = unsafe {
                &mut *n
            };
            self.next = ptr.next.take();
            let mut b = unsafe {Box::from_raw(ptr)};
            Some(std::mem::replace(&mut *b, Self::empty()))
        } else {
            None
        }
    }

    pub fn new(k: K, v: V) -> Self {
        Self { key: k, value: Some(v), next: None }
    }
}

// #[derive(Debug)]
pub struct Dict<K, V: Default, H: Hasher=DefaultHasher>
    where K: Hash + Eq + Default + Debug, 
    V: Debug {
    main: Vec<*mut DictEntry<K, V>>,
    /// 正在 rehashing 的表
    processing: Option<Vec<*mut DictEntry<K, V>>>,
    /// 当前 size 的 exp 值, size = 1 << size_exp
    size_exp: [usize; 2],
    rehashing_idx: usize,
    /// 当前已用的
    used: usize,
    hasher_builder: fn() -> H,
    phantom_k: PhantomData<K>,
    phantom_v: PhantomData<V>,
}

impl<K, V, H> Drop for Dict<K, V, H>
    where K: Hash + Eq + Default + Debug,
    V: Default + Debug,
    H: Hasher {
    fn drop(&mut self) {
        // println!("before drop: {:?}", self);
        let mut back_vec = self.processing.take();
        // println!("drop back vec now: {:?}", back_vec);
        if back_vec.is_some() {
            let back = back_vec.as_mut().unwrap();
            // let mut idxes = Vec::new();
            for (idx, &cur) in back.iter().enumerate() {
                if cur.is_null() {
                    continue
                }
                unsafe {
                    let _ = Box::from_raw(cur);
                };
                // println!("after drop {:?} in back", idx);
                // idxes.push(idx);
            }
            // for idx in idxes {
                // back[idx] = std::ptr::null_mut();
            // }
            // println!("after drop back, {:?}", back);
        }
        self.processing = None;
        // println!("drop main vec now");
        // let mut idxes = Vec::new();
        for (idx, &cur) in self.main.iter().enumerate() {
            if cur.is_null() {
                continue;
            }
            let _ = unsafe{Box::from_raw(cur)};
            // println!("after drop {:?} in main", idx);
            // idxes.push(idx);
        }
        // for idx in idxes {
            // self.main[idx] = std::ptr::null_mut();
        // }
        // println!("after drop main, {:?}", self.main);
    }
}

impl <K, V, H> Debug for Dict<K, V, H>
    where K: Hash + Eq + Default + Debug,
    V: Default + Debug, 
    H: Hasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Dict{\n");
        f.write_fmt(format_args!("\texp={:?}\n", self.size_exp));
        f.write_fmt(format_args!("\tused={:?}\n", self.used));
        f.write_fmt(format_args!("\tmain=({:?}){{\n", self.main.len()));
        for (idx, &head) in (&self.main).iter().enumerate() {
            if !head.is_null() {
                f.write_fmt(format_args!("\t\t{:?} {:?}\n", idx, unsafe{&mut *head}));
            }
        }
        f.write_str("\t}main\n");
        if let Some(rehashing) = &self.processing {
            f.write_str(&format!("\trehashing=({:?}){{\n", rehashing.len()));
            for (idx, &head) in rehashing.iter().enumerate() {
                if !head.is_null() {
                    f.write_fmt(format_args!("\t\t{:?} {:?}\n", idx, unsafe{&mut *head}));
                } 
            }
            f.write_str("\t}rehashing\n");
        }
        f.write_str("}\n");
        Ok(())
    }
}

impl <K, V> Dict<K, V, DefaultHasher>
    where K: Hash + Eq + Default + Debug,
    V: Default + Debug {
    pub fn new() -> Self {
        let init_cap = 8;
        Self::new_with_cap(init_cap)
    }

    pub fn new_with_cap(cap: usize) -> Self {
        Self::new_with_hasher_cap(cap, DefaultHasher::new)
    }
}

impl <K, V, H> Dict<K, V, H> 
    where K: Hash + Eq + Default+Debug,
    V: Default + Debug, 
    H: Hasher {
    pub fn new_with_hasher_cap(cap: usize, hasher_builder: fn() -> H) -> Self {
        let exp = Self::min_meet_exp(cap).unwrap();
        let size = 1<<exp;
        Self { 
            main: vec![std::ptr::null_mut(); size], 
            processing: None, 
            size_exp: [exp, 0], // 0 for processing
            rehashing_idx: 0, 
            used: 0, 
            hasher_builder,
            phantom_k: PhantomData, 
            phantom_v: PhantomData,
        }
    }


    #[inline]
    fn need_rehash(&self) -> bool {
        !self.is_rehashing() && self.used >= self.main.len()
    }

    pub fn get<'a>(&'a mut self, key: &'a K) -> Option<&'a V> {
        if self.is_rehashing() {
            self.rehash(1);
        }
        if let Some(slot) = self.processing.as_ref() {
            let idx = self.hash_idx(key, self.size_exp[1]);
            Self::do_find_mut(slot, idx, key)
                .and_then(|entry| entry.get_value())
                .or_else(|| {
                let idx = self.hash_idx(key, self.size_exp[0]);
                Self::do_find_mut(&self.main, idx, key)
                    .and_then(|entry| entry.get_value())
            })
        } else {
            let idx = self.hash_idx(key, self.size_exp[0]);
            Self::do_find_mut(&self.main, idx, key)
                .and_then(|n| {
                n.get_value()
            })
        }
    }

    pub fn set(&mut self, key: K, value: V) -> Option<V> {
        if self.need_rehash() {
            self.size_exp[1] = self.size_exp[0] + 1;
            self.processing = Some(vec![std::ptr::null_mut(); 1 << self.size_exp[1]]);
        }
        if self.is_rehashing() {
            self.rehash(1);
        }
        // println!("before set: {:?}", self);
        let mut ori_from_main = None;
        if self.is_rehashing() {
            if let Some(mut o) = self.remove_from_main(&key) {
                ori_from_main = o.take_value();
            }
        }
        let entry = self.insert_at(key);
        let ori = entry.replace_value(value);
        // let ori = Some(V::default());
        if ori.is_none() && ori_from_main.is_none() {
            self.used += 1;
        }
        if ori.is_some() {
            ori
        } else {
            ori_from_main
        }
    }

    fn do_find_mut<'a>(slot: &'a Vec<*mut DictEntry<K, V>>, idx: usize, key: &'a K) -> Option<&'a mut DictEntry<K, V>> {
        if slot[idx].is_null() {
            return None
        }
        let hit = slot[idx] as *mut DictEntry<K, V>;
        let hit = unsafe {&mut *hit}; 
        hit.find_mut(key)
    }

    fn remove_from_slots(slot: &mut Vec<*mut DictEntry<K, V>>, idx: usize, key: &K) -> Option<DictEntry<K, V>> {
        if slot[idx].is_null() {
            return None
        }
        let hit = slot[idx] as *mut DictEntry<K, V>;
        let hit = unsafe {&mut *hit};
        if hit.key == *key {
            if let Some(next) = hit.next {
                slot[idx] = next;
                hit.next = None;
            } else {
                slot[idx] = std::ptr::null_mut();
            }
            let mut tmp = unsafe{Box::from_raw(hit)};
            return Some(std::mem::replace(&mut *tmp, DictEntry::empty()))
        } else {
            hit.remove_tail(key)
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.is_rehashing() {
            self.rehash(1);
        }
        let result = if self.is_rehashing() {
            let mut ori = self.remove_from_rehash_slots(key);
            if ori.is_none() {
                ori = self.remove_from_main(key);
            }
            ori
        } else {
            self.remove_from_main(key)
        };
        // println!("removed({:?}) = {:?}", *key, result);
        if result.is_some() {
            self.used -= 1;
        }
        result.and_then(|mut entry| {
            entry.take_value()
        })
    }

    fn remove_from_rehash_slots(&mut self, key: &K) -> Option<DictEntry<K, V>> {
        if !self.is_rehashing() {
            None
        } else {
            let idx = self.hash_idx(key, self.size_exp[1]); 
            Self::remove_from_slots(self.processing.as_mut().unwrap(), idx, key)
        }
    }

    fn remove_from_main(&mut self, key: &K) -> Option<DictEntry<K, V>> {
        let idx = self.hash_idx(key, self.size_exp[0]);
        Self::remove_from_slots(&mut self.main, idx, key)
    }

    #[inline]
    fn size_mask(exp: usize) -> usize {
        (1 << exp) - 1
    }

    #[inline]
    fn hash_idx(&self, k: &K, exp: usize) -> usize {
        let mut hasher = (self.hasher_builder)();
        k.hash(&mut hasher);
        let hash_val = hasher.finish() as usize;
        hash_val & Self::size_mask(exp)
    }

    #[inline]
    fn min_meet_exp(size: usize) -> Option<usize> {
        for exp in 0..64 {
            if 1 << exp >= size {
                return Some(exp)
            }
        }
        None
    }

    fn is_rehashing(&self) -> bool {
        self.processing.is_some()
    }

    fn insert_at(&mut self, k: K) -> &mut DictEntry<K, V> {
        let rehashing = self.is_rehashing();
        let idx = self.hash_idx(&k, self.size_exp[if rehashing {1} else {0}]);
        let slots = if let Some(back) = &mut self.processing {
            back
        } else {
            &mut self.main
        };
        if slots[idx].is_null() {
            let entry = Box::new(DictEntry{
                key: k,
                value: Default::default(),
                next: None,
            });
            let ptr = Box::into_raw(entry);
            slots[idx] = ptr;
            return unsafe{&mut *ptr};
        } else {
            let hit = slots[idx] as *mut DictEntry<K, V>;
            let hit = unsafe {&mut *hit};
            if let Some(origin) = hit.find_mut(&k) {
                return &mut *origin;
            } else {
                let entry = Box::new(DictEntry{
                    key: k,
                    value: Default::default(),
                    next: Some(slots[idx]),
                });
                let ptr = Box::into_raw(entry);
                slots[idx] = ptr;
                return unsafe{&mut *ptr};
            }
        }
    }

    fn rehash(&mut self, step: usize) {
        for _ in 0..step {
            loop {
                if self.rehashing_idx >= self.main.len() {
                    // over
                    println!("rehash over, main: {:?}", self.main);
                    println!("rehash over, processing: {:?}", self.processing.as_ref());
                    self.main = self.processing.take().unwrap();
                    self.size_exp[0] = self.size_exp[1];
                    self.rehashing_idx = 0;
                    return;
                }
                let mut acted = false;
                let cursor = self.main[self.rehashing_idx];
                if !cursor.is_null() {
                    let cur = unsafe{&mut *cursor};
                    while let Some(mut n) = cur.take_next() {
                        let key = n.take_key();
                        let value = n.take_value();
                        let entry = self.insert_at(key);
                        entry.value = value;
                    }
                    let key = cur.take_key();
                    let value = cur.take_value();
                    let entry = self.insert_at(key);
                    entry.value = value;
                    let _ = unsafe {
                        Box::from_raw(cur)
                    };
                    self.main[self.rehashing_idx] = std::ptr::null_mut();
                    acted = true;
                }
                self.rehashing_idx += 1;
                if acted {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Dict;

    use super::DictEntry;

    #[test]
    fn entry_test() {
        let mut head = DictEntry::new(3, 3);
        head.insert(DictEntry::new(4, 4));
        head.insert(DictEntry::new(5, 5));
        assert_eq!((*head.find_mut(&4).unwrap()).get_value().unwrap(), &4);
        assert_eq!((*head.find_mut(&5).unwrap()).get_value().unwrap(), &5);
        assert_eq!((*head.find_mut(&3).unwrap()).get_value().unwrap(), &3);
        assert!(head.find_mut(&2).is_none());
        assert_eq!(*head.remove_tail(&4).unwrap().get_value().unwrap(), 4);
        assert_eq!(*head.remove_tail(&5).unwrap().get_value().unwrap(), 5);
        assert!(head.find_mut(&4).is_none());
        assert!(head.find_mut(&5).is_none());
        assert_eq!((*head.find_mut(&3).unwrap()).get_value().unwrap(), &3);
        assert!(head.remove_tail(&3).is_none());
    }

    #[test]
    fn leak() {
        let mut dict = Dict::<i32, i32>::new_with_cap(1);
        dict.set(3, 3);
        dict.set(5, 4);
        assert_eq!(dict.set(5, 5).unwrap(), 4);
        dict.set(1, 1);
        dict.set(6, 6);
        assert_eq!(dict.get(&6).unwrap(), &6);
        dict.get(&6);
        println!("get ==> {:?}", dict);
        dict.get(&6);
        println!("get ==> {:?}", dict);
        dict.get(&6);

        assert_eq!(dict.set(3, 4).unwrap(), 3);
        println!("set ==> {:?}", dict);
        assert!(dict.get(&2).is_none());
        println!("get ==> {:?}", dict);
        // println!("drop now... {:?}", dict);
    }

    #[test]
    fn hash_test() {
        let mut dict = Dict::new_with_cap(1);
        assert_eq!(dict.main.len(), 1);
        assert!(!dict.need_rehash());
        assert!(dict.set(3, 3).is_none());
        assert_eq!(dict.set(3, 4).unwrap(), 3);
        assert!(dict.set(4, 4).is_none());
        println!("dict = {:?}", dict);
        assert_eq!(dict.used, 2);

        dict.set(5, 5);
        assert!(dict.is_rehashing());
        dict.set(6, 6);
        println!("{:?}", dict);
        // assert_eq!(dict.main.len(), 2);

        // assert_eq!()
        assert_eq!(*dict.get(&3).unwrap(), 4);
        dict.remove(&4);
        println!("{:?}", dict);
        dict.remove(&6);
        println!("{:?}", dict);
        dict.remove(&3);
        println!("{:?}", dict);
        // dict.remove(&5);
        // println!("{:?}", dict);
        assert_eq!(dict.remove(&5).unwrap(), 5);
    }
}