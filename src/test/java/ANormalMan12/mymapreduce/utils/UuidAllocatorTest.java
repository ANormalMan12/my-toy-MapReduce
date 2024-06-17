package ANormalMan12.mymapreduce.utils;

import junit.framework.TestCase;

import java.util.UUID;

public class UuidAllocatorTest extends TestCase {

    public void testAllocateUUID() {
        UuidAllocator uuidAllocator =new UuidAllocator();
        UUID uuid = uuidAllocator.allocateUUID();
        System.out.println(uuid);
    }
}