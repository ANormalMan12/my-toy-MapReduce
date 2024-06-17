package ANormalMan12.mymapreduce.utils;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class UuidAllocator {
    private final ConcurrentSkipListSet<UUID> uuidSet = new ConcurrentSkipListSet<>();

    public synchronized UUID allocateUUID() {
        UUID uuid = UUID.randomUUID();
        System.out.println("Allocate " + uuid.toString());
        while (!uuidSet.add(uuid)) {
            // 如果UUID已经存在，则重新生成
            uuid = UUID.randomUUID();
        }
        return uuid;
    }

    public synchronized void deallocateUUID(UUID uuid) {
        uuidSet.remove(uuid);
    }
}