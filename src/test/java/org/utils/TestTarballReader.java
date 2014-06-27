package org.utils;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.kamranzafar.jtar.TarInputStream;

/**
 * TestTarballReader.
 *
 * Under Apache License 2.0 
 * 
 * @author pgrandjean
 * @date 27 Jun 2014
 * @since 1.6.x
 */
public class TestTarballReader {

    @Test
    public void test01() throws Exception {
        InputStream in = this.getClass().getResourceAsStream("/test.tar.gz");
        GZIPInputStream gzip = new GZIPInputStream(in);
        TarInputStream tar = new TarInputStream(gzip);
        
        TarballReader tarballReader = new TarballReader(tar);

        int numberOfFiles = 0;
        while (tarballReader.nextKeyValue()) numberOfFiles++;

        assertEquals(10, numberOfFiles);
        
        tar.close();
    }

    @Test
    public void test02() throws Exception {
        InputStream in = this.getClass().getResourceAsStream("/test.tar.gz");
        GZIPInputStream gzip = new GZIPInputStream(in);
        TarInputStream tar = new TarInputStream(gzip);
        
        TarballReader tarballReader = new TarballReader(tar);

        int[] sizes = { 8069, 6112, 10638, 8258, 9280, 10080, 9669, 11767, 13255, 13875 };
        int i = 0;
        while (tarballReader.nextKeyValue()) {
            Text key = tarballReader.getCurrentKey();
            Text value = tarballReader.getCurrentValue();
            
            assertTrue(key.toString().matches(".*.txt.D\\d{6}.T\\d{6}"));
            assertEquals(sizes[i++], value.getLength());
        }
        
        tar.close();
    }
}
