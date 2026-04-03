box.cfg{
    listen = 3301
}

box.schema.user.create('app', {
    password = 'secret',
    if_not_exists = true
})

box.schema.user.enable('app')

box.schema.user.grant('app', 'read,write,execute', 'universe', nil, {
    if_not_exists = true
})

local kv = box.space.KV

if kv == nil then
    kv = box.schema.space.create('KV', {
        if_not_exists = true
    })

    kv:format({
        { name = 'key', type = 'string' },
        { name = 'value', type = 'varbinary', is_nullable = true }
    })

    kv:create_index('primary', {
        parts = {
            { field = 'key', type = 'string' }
        },
        if_not_exists = true
    })
end

require('console').start()