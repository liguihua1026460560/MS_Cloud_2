package com.macrosan.utils.serialize;

import com.macrosan.message.xmlmsg.Error;
import com.macrosan.message.xmlmsg.ListBucketResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.List;
import java.util.Optional;

import static com.macrosan.constants.ServerConstants.XML_PACKAGE_NAME;
import static com.macrosan.utils.cache.ClassUtils.getClassFlux;
import static com.macrosan.utils.functional.exception.ThrowingConsumer.throwingConsumerWrapper;

/**
 * JaxbUtils
 * 提供xml的序列化和反序列化方法，由jaxb实现
 * <p>
 * 目前Marshaller和Unmarshaller未做缓存，因为需要序列化和反序列化xml的次数不多，为此牺牲内存不划算
 *
 * @author liyixin
 * @date 2018/12/6
 */
public class JaxbUtils {

    private static final Logger logger = LogManager.getLogger(JaxbUtils.class.getName());

    private static JAXBContext jaxbContext;
    private static JAXBContext errorJaxContext;

    public static void initJaxb() {
        List<? extends Class<?>> classList = getClassFlux(XML_PACKAGE_NAME, ".class")
                .filter(cls -> !"package-info".equals(cls.getSimpleName()))
                .collectList().block();
        Optional.ofNullable(classList).ifPresent(throwingConsumerWrapper(list ->
                jaxbContext = JAXBContext.newInstance(list.toArray(new Class[0])))
        );


        Optional.ofNullable(classList).ifPresent(throwingConsumerWrapper(list ->
                errorJaxContext = JAXBContext.newInstance(Error.class))
        );
    }

    private JaxbUtils() {
    }

    public static byte[] toByteArray(Object msg) {
        try {
            Marshaller m = jaxbContext.createMarshaller();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            m.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
            m.setProperty("com.sun.xml.internal.bind.xmlHeaders",
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            m.setListener(new Marshaller.Listener() {
                @Override
                public void beforeMarshal(Object source) {
                    if (source instanceof ListBucketResult){
                        ListBucketResult listBucketResult = (ListBucketResult)source;
                        reduceData(listBucketResult);
                    }
                }
            });
            m.marshal(msg, outputStream);
            return outputStream.toByteArray();
        } catch (JAXBException e) {
            logger.error("marshaller msg fail :", e);
            return new byte[0];
        }
    }

    public static void reduceData(ListBucketResult listBucketResult){
        if (StringUtils.isNotBlank(listBucketResult.getListType())){
            if (listBucketResult.isFetchOwner()){
                listBucketResult.getContents().forEach(contents -> {
                    contents.setOwner(null);
                });
            }
            if (StringUtils.isBlank(listBucketResult.getContinuationToken())){
                listBucketResult.setContinuationToken(null);
            }
            if (StringUtils.isBlank(listBucketResult.getStartAfter())){
                listBucketResult.setStartAfter(null);
            }
            if (StringUtils.isBlank(listBucketResult.getDelimiter())){
                listBucketResult.setDelimiter(null);
            }
            listBucketResult.setNextMarker(null);
            listBucketResult.setMarker(null);
            listBucketResult.setKeyCount(String.valueOf(listBucketResult.getContents().size()+listBucketResult.getPrefixlist().size()));
        }else {
            listBucketResult.setKeyCount(null);
        }
    }

    public static Object toObject(String str) {
        try {
            Unmarshaller m = jaxbContext.createUnmarshaller();
            StringReader reader = new StringReader(str);
            SAXParserFactory sax = SAXParserFactory.newInstance();
            sax.setNamespaceAware(false);
            XMLReader xmlReader = sax.newSAXParser().getXMLReader();
            Source source = new SAXSource(xmlReader, new InputSource(reader));
            return m.unmarshal(source);
        } catch (JAXBException | SAXException | ParserConfigurationException e) {
            logger.error("unmarshaller msg fail :", e);
            return null;
        }
    }

    public static Object toErrorObject(String str) {
        try {
            Unmarshaller m = errorJaxContext.createUnmarshaller();
            StringReader reader = new StringReader(str);
            SAXParserFactory sax = SAXParserFactory.newInstance();
            sax.setNamespaceAware(false);
            XMLReader xmlReader = sax.newSAXParser().getXMLReader();
            Source source = new SAXSource(xmlReader, new InputSource(reader));
            return m.unmarshal(source);
        } catch (JAXBException | SAXException | ParserConfigurationException e) {
            logger.error("unmarshaller msg fail :", e);
            return null;
        }
    }
}
