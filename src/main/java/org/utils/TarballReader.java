package org.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;

/**
 * TarballReader.
 *
 * Outputs for file included in a tarball a key/value pair where the key is
 * the file name appended with date and time (.DYYMMDD.THHMMSS) and the value
 * is the content of the file.
 *
 * Under Apache License 2.0 
 * 
 * @author pgrandjean
 * @date 27 Jun 2014
 * @since 1.6.x
 */
public class TarballReader extends RecordReader<Text, Text> {

    private SimpleDateFormat dateFormatter = new SimpleDateFormat("'D'yyMMdd.'T'HHmmss");
    
    private long pos = 0;

    private long end = 0;
    
    private TarInputStream in = null;
    
    private Text key = null;

    private Text value = null;

    public TarballReader() {}

    protected TarballReader(TarInputStream in) {
        this.in = in;
        this.key = new Text();
        this.value = new Text();
    }
    
    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            key = null;
            value = null;
        }
    }

    @Override
    public synchronized boolean nextKeyValue() throws IOException {
        TarEntry tarEntry = in.getNextEntry();
        if (tarEntry == null) return false;
        
        // clear K/V
        key.clear();
        value.clear();
        
        // provide timestamp of file creation as a temp solution for
        // missing UNB segments in DCS FM
        
        Calendar timestamp = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        timestamp.setTimeInMillis(tarEntry.getModTime().getTime());
        
        key.set(tarEntry.getName() + "." + dateFormatter.format(timestamp.getTime()));

        // read tar entry
        long tarSize = tarEntry.getSize();
        if (tarSize > Integer.MAX_VALUE) throw new IOException("tar entry " + tarEntry.getName() + " exceeds " + Integer.MAX_VALUE);
        
        int bufSize = (int) tarSize;
        int read = 0;
        int offset = 0;
        byte[] buffer = new byte[bufSize];
        
        while ((read = in.read(buffer, offset, bufSize)) != -1) offset += read;
        
        // set value
        value.set(buffer);
        
        // set pos
        pos += bufSize;
        
        if (value.getLength() == 0) {
            key = null;
            value = null;
            return false;
        }
        
        return true;
    }

    @Override
    public synchronized Text getCurrentKey() {
        return key;
    }

    @Override
    public synchronized Text getCurrentValue() {
        return value;
    }

    @Override
    public synchronized float getProgress() throws IOException {
        return Math.min(1.0f, pos / (float) end);
    }

    @Override
    public void initialize(InputSplit isplit, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            pos = 0;
            end = Long.MAX_VALUE;
            key = new Text();
            value = new Text();

            FileSplit split = (FileSplit) isplit;
            Path file = split.getPath();
            
            Configuration conf = context.getConfiguration();
            CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
            CompressionCodec codec = compressionCodecs.getCodec(file);

            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream fileIn = fs.open(split.getPath());
            
            in = new TarInputStream(codec.createInputStream(fileIn));
        }
        catch (IOException ex) {
            Logger.getLogger(TarballReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
