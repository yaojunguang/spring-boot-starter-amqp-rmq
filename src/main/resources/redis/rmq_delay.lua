local offset=0;
local info = {};
local keyDelay = KEYS[1]..ARGV[1];
--循环将zset中到时间的数据移动到stream中
while(true)
do
    -- 每次取50条
    local items=redis.call('ZRANGEBYSCORE',keyDelay,0,ARGV[2],'WITHSCORES','LIMIT',offset,50);
    if (#items > 0) then
        offset = offset + #items;
        for i=1, #items,2 do
            -- 为防止member因为重复而丢失消息，这里将原始消息拼接上 [唯一id]==delayId:,放入stream时去除
            local data = items[i];
            local index = string.find(data, ':', 0, true);
            index = index or -1;
            local delayId = '';
            if(index > 0) then
                delayId = string.sub(data,0, index-10);
                data = string.sub(data, index+1);
            end
            -- 将消息放入stream，并记录日志
            local id = redis.call('XADD',KEYS[1],'*','payload',data);
            table.insert(info, 'delayId=' .. delayId .. ',id=' .. id);
        end
    end
    if #items < 50 then
        break;
    end
end
-- 移出已经转移到消息队列中的数据
if(offset > 0) then
    redis.call('ZREMRANGEBYSCORE',keyDelay,0,ARGV[2]);
end
return cjson.encode(info);
