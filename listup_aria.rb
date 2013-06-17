#!/usr/bin/env ruby

require 'logger'
require 'sqlite3'
require 'zlib'

if ARGV.length < 3 || (ARGV[2] != "text" && ARGV[2] != "db") then
  puts "Usage: listup_aria.rb [avs_file] [list_file] [ text | db ]"
  exit 1
end

print "Start: "
p Time.now

log = Logger.new("listup_aria.log")

prev_offset = 0
count = 0

if ARGV[2] == "text" then
  op = File.open(ARGV[1] + ".lst",  "w")
elsif ARGV[2] == "db" then
  db = SQLite3::Database.new(ARGV[1] + ".db")
  sql = <<SQL
create table if not exists keys (
  key varchar(256),
  clock integer,
  del integer,
  flag integer
);
SQL
  db.execute(sql)
  db.transaction
end
io = File.open(ARGV[0], "rb")

io.read(111)

while ! io.eof? do
  crc32 = io.read(4).unpack("H*")[0].hex.to_i

  header = io.read(29)
  ksize = header[0, 1].unpack("H*")[0].hex.to_i
  dsize = header[1, 4].unpack("H*")[0].hex.to_i
  offset = header[9, 4].unpack("H*")[0].hex.to_i
  vnode = header[13, 4].unpack("H*")[0].hex.to_i
  vclock = header[17, 4].unpack("H*")[0].hex.to_i
  del = header[28, 1].unpack("H*")[0].hex.to_i
  key = io.read(ksize)
  body = io.read(dsize)
  bin = header[0, 5] + header[13, 8] + key + body

  ret_crc32 = Zlib.crc32(bin, 0)

  if crc32 == ret_crc32 && io.read(8) == "\0\0\0\0\0\0\0\0" then
    prev_offset = io.pos
    if ARGV[2] == "text" then
      if key + ",0,0,0\n" != ",0,0,0\n" then
        op.write(key + ",0,0,0\n")
      else
        log.error("key is not exists. #{offset}")
      end
      count = count + 1
    elsif ARGV[2] == "db" then
      sql = "select key, clock from keys where key = ?"
      ret = db.execute(sql, key)
      if ret.length == 0 then
        sql = "insert into keys values (?,?,?,?)"
        db.execute(sql, key, vclock, del, 0)
        count = count + 1
      elsif ret[0][1].to_i < vclock then
        sql = "update keys set clock = ?, del = ? where key = ?"
        db.execute(sql, vclock, del, key)
      end
    end
    log.info("#{prev_offset} #{key}")
  else
    prefix = 1
    io.seek(prev_offset, IO::SEEK_SET)
    while io.read(8) != "\0\0\0\0\0\0\0\0" do
      io.seek(prev_offset + prefix, IO::SEEK_SET)
      prefix = prefix + 1
    end
    prev_offset = io.pos
    log.error("#{prev_offset} #{key} src_crc32: #{crc32} dst_crc32: #{ret_crc32}")
  end
end

io.close()
if ARGV[2] == "text" then
  op.close()
elsif ARGV[2] == "db" then
  db.commit
  db.close()
end

print "End  : "
p Time.now
puts "Total: " + count.to_s + " Offset: " + prev_offset.to_s
exit 0
