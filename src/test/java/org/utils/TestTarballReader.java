package org.utils;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.util.zip.GZIPInputStream;

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
        InputStream in = this.getClass().getResourceAsStream("/20140623-121530.PRD.WRH.FM.AMA.ONE.FTP.DATA.tar.gz");
        GZIPInputStream gzip = new GZIPInputStream(in);
        TarInputStream tar = new TarInputStream(gzip);
        
        TarballReader tarballReader = new TarballReader(tar);

        int numberOfFiles = 0;
        while (tarballReader.nextKeyValue()) numberOfFiles++;

        assertEquals(45, numberOfFiles);
        
        tar.close();
    }
}
