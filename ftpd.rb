#!ruby
#
# FTP daemon
#
# Copyright (C) 2013 norsan
#

require "yaml"
require "socket"
require "thread"
require "timeout"

# read ftpd.yml for initializing parameters.
inidata = YAML::load_file(File.dirname(__FILE__) + "/ftpd.yml")
$rootdir = inidata["rootdir"] || "/"
$users = inidata["users"] || {}
$bufsize = inidata["bufsize"] || 65536
$ftp = inidata["ftp"] || 21
$ftpdata = inidata["ftpdata"] || 20
$dputs = inidata["dputs"] || false

def dputs(*args)
  puts(*args) if $dputs
end

class FTPDaemon

  class AbortException < StandardError
  end
  
  def initialize(s)
    @username = nil
    @loggedin = false
    @datatype = "ASCII"
    @pasv = nil
    @port = nil
    @rootdir = nil
    @wd = "/"
    @client = s
    @abort = false
  end
  
  def start
    dputs "start: #{@client.addr.inspect}"
    
    queue = Queue.new
    thread = Thread.start do
      begin
        while true
          cmd, param = queue.deq
          exec_cmd(cmd, param)
        end
      rescue Exception
        dputs "data_connection exception: #{$!.inspect}" + $!.backtrace.join("\n")
      end
    end
    
    resp(220)
    while true
      cmd, param = wait_cmd
      break if cmd.nil?
      dputs "cmd:#{cmd}, param:#{param}"
      
      # common commands
      case cmd
      when "QUIT"
        resp(221)
        break
      when "USER"
        @loggedin = false
        @username = param
        resp(331)
        next
      when "PASS"
        if @username.nil?
          resp(503)
        else
          pass = $users[@username]
          if param.crypt(pass) != pass
            @username = nil
            resp(530, "Login incorrect.")
          else
            @rootdir = File.join($rootdir, @username)
            unless File.exists?(@rootdir)
              @username = nil
              resp(530, "Login incorrect.")
            else
              @loggedin = true
              resp(230)
            end
          end
        end
        next
      when "ABOR"
        @abort = true
        dputs "ABOR received."
        next
      end
      
      # send general command
      queue.enq([ cmd, param ])
    end
    
    thread.kill.join
    nil
  end
  
  def wait_cmd
    str = @client.gets
    if str.nil?
      nil
    else
      str.chomp.split(/ +/, 2)
    end
  end
  
  # execute general command
  def exec_cmd(cmd, param)
    unless @loggedin
      resp(530)
      return
    end
    case cmd
    when "CDUP"
      @wd.sub!(/\/[^\/]+$/, "")
      @wd = "/" if @wd.empty?
      change_wd(cmd)
    when "CWD"
      if param[0] == '/' # @wd set root-path(/) if param is absolute path.
        @wd = "/"
      end
      param.split("/").each do |path|
        case path
        when "", "." then nil
        when ".."    then @wd.sub!(/[^\/]+\/$/, "")
        else              @wd << "#{path}/"
        end
      end
      @wd.gsub!(/\/{2,}/, '/') # multiple slashs replace single slash.
      @wd.gsub!(/\/$/, '') unless @wd == "/"
      change_wd(cmd)
    when "PWD"
      resp(257)
    when "TYPE"
      case param
      when "A"
        @datatype = "ASCII"
        resp(200, param)
      when "I"
        @datatype = "BINARY"
        resp(200, param)
      else
        resp(501)
      end
    when "PORT"
      if @pasv
        resp(500, "Illegal PORT command")
      else
        data = param.split(",").map(&:to_i)
        port = (data[4] << 8) + data[5]
        addr = data[0..3].join(".")
        @port = [ addr, port ]
        resp(250, cmd)
      end
    when "PASV"
      if @port
        resp(500, "Illegal PASV command")
      else
        @pasv.close if @pasv
        @pasv = TCPServer.new(0)
        port = @pasv.addr[1].to_i
        addr = IPSocket.getaddress(Socket::gethostname)
        data = addr.split(".").map(&:to_i)
        data << (port >> 8) << (port % 256)
        resp(227, data.join(","))
      end
    when "LIST"
      data_connection(cmd) do |sock|
        wd = File.join(@rootdir, @wd)
        Dir.entries(wd).each do |name|
          next if name == "." || name == ".."
          fname = File.join(wd, name)
          mt = File.mtime(fname)
          dr = File.directory?(fname)
          sz = File.size(fname)
          msg = format(
            "%srwxrwxrwx 1 owner group #{sz} %s %s #{name}",
            dr ? 'd' : '-',
            mt.strftime("%b %d"),
            mt.strftime((Time.now - mt > 60*60*24*6) ? "%Y" : "%H:%M")
          )
          break if @abort
          sock.puts msg
        end
      end
    when "RETR" # get
      data_connection(cmd) do |sock|
        File.open(absolute_path(param), "rb") do |fp|
          buf = " " * ($bufsize + 1)
          while fp.read($bufsize, buf)
            raise AbortException,"" if @abort
            #dputs "RETR: #{$bufsize} transferd."
            sock.write(buf)
          end
        end
      end
    when "STOR" # put
      data_connection(cmd) do |sock|
        File.open(absolute_path(param), "wb") do |fp|
          buf = " " * ($bufsize + 1)
          while sock.read($bufsize, buf)
            raise AbortException,"" if @abort
            #dputs "STOR: #{$bufsize} tranfered."
            fp.write(buf)
          end
        end
      end
    else
      resp(502)
    end
  end
  
  def data_connection(cmd)
    sock = nil
    if @port
      #timeout(60) { sock = TCPSocket.new(@port[0], @port[1], "localhost", $ftpdata) }
      timeout(60) { sock = TCPSocket.new(*@port) }
      resp(150, cmd)
      yield sock
      resp(226)
    elsif @pasv
      resp(150, cmd)
      timeout(60) { sock = @pasv.accept }
      yield sock
      resp(226)
    else
      resp(425)
    end
  rescue SocketError
    resp(425, @port.inspect)
  rescue TimeoutError, AbortException
    dputs "data_connection: #{$!.inspect}" + $!.backtrace.join("\n")
    resp(551, "Request action aborted.")
  ensure
    sock.close if sock
    @pasv.close if @pasv
    @pasv = nil
    @port = nil
  end
  
  def absolute_path(path)
    if path =~ /^\//
      File.join(@rootdir, path)
    else
      File.join(@rootdir, @wd, path)
    end
  end
  
  def change_wd(cmd)
    dputs "change_wd: #{@rootdir}, #{@wd}"
    if Dir.exist?(absolute_path(@wd))
      resp(250, cmd)
    else
      resp(550, @wd)
    end
  end
  
  def resp(num, msg = "")
    msg =
    case num
      # 1xx
    when 150 then "Opening #{@datatype} mode data connection for #{msg}"
      # 2xx
    when 200 then "Type set to #{msg}"
    when 220 then "#{IPSocket.getaddress(Socket::gethostname)} FTP server ready"
    when 221 then "Goodbye"
    when 226 then "Transfer complete"
    when 227 then "Entering Passive Mode(#{msg})"
    when 230 then "User #{@username} logged in."
    when 250 then "#{msg} command successful"
    when 257 then "\"#{@wd}\" is current directory"
      # 3xx
    when 331 then "Password required"
      # 4xx
    when 425 then "Can't open data connection [#{msg}]"
      # 5xx
    when 500 then (msg || "Syntax error, command, unrecognized")
    when 501 then (msg || "Syntax error in parameters or arguments")
    when 502 then "Command no implemented"
    when 503 then "Login with USER first"
    when 530 then (msg || "Not Logged in")
    when 550 then "No such file or directory"
    else raise RuntimeError, "unknown response [#{num}]"
    end
    str = "#{num} #{msg}"
    dputs "resp: #{str}"
    @client.puts str
    nil
  end
end

Thread.abort_on_exception = true
ftps = TCPServer.new($ftp)
dputs "ftp server started."
while true
  Thread.start(ftps.accept) do |s|
    s.setsockopt(Socket::SOL_SOCKET, Socket::SO_KEEPALIVE, true)
    begin
      FTPDaemon.new(s).start
    rescue Exception
      dputs "client exception: #{$!.inspect}" + $!.backtrace.join("\n")
    end
    s.close
  end
end

