/**
 *
 */
package com.shipco.phoenix.ecommerce.webservice.schedule.server.util.schedulelookupcache;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.core.Response;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.scanit.phoenix.common.shared.bean.PhoenixSystemUserBean;

import com.shipco.mobileapp.util.common.EnumRequestURL;
import com.scanit.mobileapp.util.common.StringUtility;
import com.shipco.phoenix.autonotification.finance.custinvoicestatementnotification.beans.ObjectResultJson;
import com.shipco.phoenix.common.email.template.dao.EmailTemplateDao;
import com.shipco.phoenix.common.shipmentstatus.beans.ShipmentStatusEventBean;
import com.shipco.phoenix.common.shipmentstatus.beans.ShipmentStatusUpdateBean;
import com.shipco.phoenix.common.shipmentstatus.service.ShipmentStatusService;
import com.shipco.phoenix.core.server.util.SpringUtil;
import com.shipco.phoenix.core.server.util.configuration.LoadConfigurationFiles;
import com.shipco.phoenix.core.server.util.db.DbConstant;
import com.shipco.phoenix.core.server.util.db.QueryReader;
import com.shipco.phoenix.core.server.util.db.stateless.SqlUtil;
import com.shipco.phoenix.core.server.util.email.EmailUtil;
import com.shipco.phoenix.core.server.util.http.HttpRequestUtil;
import com.shipco.phoenix.core.server.util.log.BaseLogger;
import com.shipco.phoenix.core.server.util.log.MessageLogger;
import com.scanit.phoenix.core.server.util.restutil.RequestUrlEnum;
import com.scanit.phoenix.restutil.AutoRetryRestUtil;
import com.shipco.phoenix.core.shared.exception.BusinessException;
import com.shipco.phoenix.core.shared.exception.FatalException;
import com.shipco.phoenix.core.shared.exception.SystemException;
import com.shipco.phoenix.ecommerce.webservice.schedule.service.impl.ScheduleXmlParserForDestination;
import com.shipco.phoenix.ecommerce.webservice.schedule.service.impl.ScheduleXmlParserForOrigin;
import com.shipco.phoenix.ecommerce.webservice.schedule.shared.beans.lookupcache.ScheduleLookUpCacheBean;
import com.shipco.phoenix.ecommerce.webservice.schedule.shared.beans.lookupcache.ScheduleLookUpLocationBean;
import com.shipco.phoenix.ecommerce.webservice.schedule.shared.beans.lookupcache.ScheduleLookUpSearchBean;
import com.shipco.phoenix.exceptionlogger.bean.ExceptionDbLogBean;
import com.shipco.phoenix.exceptionlogger.service.ExceptionDbLogService;
import com.shipco.phoenix.module.transmission.service.GenApplicationSettingService;
import com.shipco.phoenix.module.transmission.service.GenApplicationSettingService.GenApplicationSettingKey;

/**
 * Class Which used to Cache the Schedule infomation from WWA WebSerivce Side to
 * Phoenix's Table.
 *
 * @author Apichart Sae Ueng <sapichart@shipco.com>
 * @since Jun 21, 2012
 */
@DisallowConcurrentExecution
public class ScheduleLookupCacher extends QuartzJobBean {

	private static GenApplicationSettingService genApplicationSettingService;

	private static EmailTemplateDao emailtemplate;

	// Start date can be BackWard with Negative Value.
	static final int CACHE_START_DATE = -1;

	// End date can be Normally Forwarding, Positive Value.
	// On search criteria it is default at start[0] to End[60], We make here -1
	// to 65 to make sure that Default is in range, so every Constructor of that
	// Schedule criteria is called it will within the range.
	static final int CACHE_END_DATE = 65;

	// Maximum retry for http request.
	static final int REQUEST_RETRY_COUNT = 3;

	private final String SAILING_SCHEDULE_LOOKUP_READ_TIMEOUT = "SAILING_SCHEDULE_LOOKUP_READ_TIMEOUT";
	// Added by mkumar PHX-56461
	public static final String ACRARATE_DATABASE = "ACRARATE_DATABASE";
	private SqlUtil sqlUtil;
	// AddedBySumeet for PHX-76541
	private static final String SAILING_SCHEDULE_THREAD_POOL_SIZE = "SAILING_SCHEDULE_THREAD_POOL_SIZE";
	private static final String SAILING_SCHEDULE_NOTIFICATION_MAIL_ID = "SAILING_SCHEDULE_NOTIFICATION_MAIL_ID";
	public static final String POSTGRESQL = "PostgreSQL";
	public static final String TEN_STRING = "10";
	public static final String TWO_HOURS_WAIT = "120";
	public static final String SAILING_SHEDULE_DATABASE = "SAILING_SHEDULE_DATABASE";
	public static final String SAILING_SCHEDULE_TEMPLATE_FOR_NOTIFICATION = "SAILING_SCHEDULE_TEMPLATE_FOR_NOTIFICATION";
	// End

	private final static BaseLogger log = MessageLogger.getLogger();

	//public ScheduleLookUpCacheTranDao scheduleLookUpCacheTranDao;

	// Added by Piyawut 15 Aug 2012
	// Declared variable to control limit data for saving
	private final int LIMIT_SIZE_TO_SAVE = 5000;

	private static AtomicInteger countScheduleCacheRecord;

	private int httpConnrctionTimeOut = 0;
	// start - Added by Shubham Barot for PHX-39129
	private PhoenixSystemUserBean userBean;
	public static final String USER_FULLNAME = "Phoenix System";
	public static final String LDAP_USERNAME = "phoenix_system";
	public static final String OBJECT_CODE = "QZ_SAILING_SCHEDULE";
	public static final String JOB_NAME = "Sailing Schedule from WWA";
	public static final String EVENT_NAME = "SAILING-SCHEDULE-COMPLETED";

	private ShipmentStatusService shipmentStatusService;
	// End

	/**
	 * Execute the Job method.
	 *
	 * @param arg0
	 * @throws JobExecutionException
	 */
	@Override
	protected void executeInternal(JobExecutionContext arg0) throws JobExecutionException {
		ExceptionDbLogService exceptionLogService = null;

		try {
			exceptionLogService = SpringUtil.getInstance().getBean(ExceptionDbLogService.class);
			sqlUtil = SpringUtil.getInstance().getBean(SqlUtil.class);
			//scheduleLookUpCacheTranDao = SpringUtil.getInstance().getBean(ScheduleLookUpCacheTranDao.class);

			List<String> configurationList = new ArrayList<String>();
			configurationList.add(SAILING_SCHEDULE_LOOKUP_READ_TIMEOUT);

			HashMap configMap = AutoRetryRestUtil.getInstance()
					.<GenGlobalConfigResultJson>sendRequest(configurationList, RequestUrlEnum.GET_GEN_GLOBAL_CONFIG,
							GenGlobalConfigResultJson.class)
					.getResult();

			httpConnrctionTimeOut = Integer.parseInt((String) configMap.get(SAILING_SCHEDULE_LOOKUP_READ_TIMEOUT));

			doScheduleLookupCacheFromWebService();

		} catch (FatalException e) {
			e.printStackTrace();
			log.error("FatalException occurred in executeInternal() by doScheduleLookupCacheFromWebService()\n "
					+ "Message\n[\n" + e.getMessage() + "\n]\n" + "StackTrace\n[\n" + e.getStackTrace() + "\n]\n");

			ExceptionDbLogBean bean = new ExceptionDbLogBean();
			bean.setProgramUnit(getClass().getName());
			bean.setThrowable(e.getCause());
			exceptionLogService.insert(bean);

		} catch (SailingScheduleException e) {
			e.printStackTrace();
			log.error("IOException occurred in executeInternal() by doScheduleLookupCacheFromWebService()\n "
					+ "Message\n[\n" + e.getMessage() + "\n]\n" + "StackTrace\n[\n" + e.getStackTrace() + "\n]\n");

			ExceptionDbLogBean bean = new ExceptionDbLogBean();
			bean.setProgramUnit(getClass().getName());
			bean.setThrowable(e.getCause());
			exceptionLogService.insert(bean);

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Exception occurred in executeInternal() by doScheduleLookupCacheFromWebService()\n "
					+ "Message\n[\n" + e.getMessage() + "\n]\n" + "StackTrace\n[\n" + e.getStackTrace() + "\n]\n", e);

			ExceptionDbLogBean bean = new ExceptionDbLogBean();
			bean.setProgramUnit(getClass().getName());
			bean.setThrowable(e.getCause());
			exceptionLogService.insert(bean);
		}

	}

	/**
	 * Collect Origin/Destination as per Range of From/To to Cache Table.
	 *
	 * @throws Exception
	 */

	// Modified by Sumeet for PHX-76541
	public void doScheduleLookupCacheFromWebService() throws Exception {
		countScheduleCacheRecord = new AtomicInteger(0);

		genApplicationSettingService = SpringUtil.getInstance().getBean(GenApplicationSettingService.class);

		// rajesh added code for Exception handling
		ExceptionDbLogService exceptionLogService = null;
		// 1. Start Cache Update.
		log.info(
				"\n\n\n\n\n\n\n================\n\n---------------------\ndoScheduleLookupCacheFromWebService() started"
						+ "\n---------------------\n\n================\n\n\n\n\n\n\n");
		boolean isResponseReceived = false;
		final long startTime = System.currentTimeMillis();
		final List<String> cachDateList = getCacheDateRangeList();
		// Added by Piyawut 15 Aug 2012
		// Adding log to show the date and time when schedule lookup stated
		log.info("\n\n---------\nStarting Time Stamp :: " + new Timestamp(startTime) + "\n---------\n\n");

		log.info("\n\n---------\nCache Date Range = [" + cachDateList.get(0) + "] to ["
				+ cachDateList.get(cachDateList.size() - 1) + "]\n---------\n\n");

		// final Connection conn =
		// sqlUtil.getConnectionFactory().getDBConnection();

		// Added for deciding the number of threads.
		String threads = genApplicationSettingService
				.getValue(GenApplicationSettingKey.SAILING_SCHEDULE_THREAD_POOL_SIZE);
		ExecutorService executor = Executors.newWorkStealingPool(
				Integer.parseInt(StringUtility.getAlternativeStringIfNullOrEmpty(threads, TEN_STRING)));

		try {
			exceptionLogService = SpringUtil.getInstance().getBean(ExceptionDbLogService.class);
			long started = System.currentTimeMillis();

			for (final String eachDateToCache : cachDateList) {
				if (!isResponseReceived) {
					String originResponseText = doRequestWWAForOrigin(eachDateToCache);
					List<ScheduleLookUpLocationBean> originList = new ScheduleXmlParserForOrigin()
							.parseDocument(originResponseText);
					if (originList.size() > 0 && !isResponseReceived) {
						isResponseReceived = true;
						if (isPostgresToggleEnable()) {
							EnumRequestURL serviceurlForClean = EnumRequestURL.SAILING_SCHEDULE_LOOKUP_TMP_CLEAN;
							sendAsynchDeletetRequestToAdminService(serviceurlForClean);
						}
					}
				}

				final executeeachdaycalls eachdatebean = new executeeachdaycalls(eachDateToCache);
				executor.execute(eachdatebean);
			}

			executor.shutdown();
			while (!executor.isTerminated()) {
			} // for (final String eachDateToCache

			final long endWSTime = System.currentTimeMillis();
			log.info("**End WebService Calls :: Time elasped : [" + (endWSTime - startTime) + "] milliSec**");

			if (isPostgresToggleEnable()) {
				EnumRequestURL serviceurlForSave = EnumRequestURL.SAILING_SCHEDULE_LOOKUP_INTERCHANGE;
				sendAdminGetRequest(serviceurlForSave.getURL());
			}
			long ended = System.currentTimeMillis();
			log.info("===== batchwise save ENDED  at " + ended + "total time taken at " + (ended - started) / 1000
					+ " secs");

			log.info("Cached Schedule Record size : [" + countScheduleCacheRecord.get() + "]\n");

			final long endSaveTime = System.currentTimeMillis();
			log.info("***End Saving/ End Caching Program :: Time for saving elasped : ["
					+ ((endSaveTime - endWSTime) / 1000) + "] Sec :: Total Time elapsed ["
					+ ((endSaveTime - startTime)) / 1000 + "] Secs***");

			// Added by Shubham Barot for PHX-39129
			userBean = new PhoenixSystemUserBean();
			userBean.setSiteId(0);
			userBean.setUserFullName(USER_FULLNAME);
			userBean.setLdapUserName(LDAP_USERNAME);
			final ShipmentStatusUpdateBean shipmentStatusUpdateBean = new ShipmentStatusUpdateBean();
			shipmentStatusUpdateBean.setObjectCode(OBJECT_CODE);
			log.info("Non Shipment object code" + shipmentStatusUpdateBean.getObjectCode());
			shipmentStatusUpdateBean.setReferenceNumber(JOB_NAME);
			final ShipmentStatusEventBean shipmentStatusEventBean = new ShipmentStatusEventBean();
			shipmentStatusEventBean.setEventName(EVENT_NAME);
			shipmentStatusEventBean.addCommandParams("JobName", JOB_NAME);
			shipmentStatusUpdateBean.addEvent(shipmentStatusEventBean);

			shipmentStatusService = SpringUtil.getInstance().getBean(ShipmentStatusService.class);

			shipmentStatusService.updateNonShipmentStatus(shipmentStatusUpdateBean, userBean);
			sendSailingScheduleEmailNotification(endWSTime - startTime, countScheduleCacheRecord.get());
			log.debug("Sailing Schedule Job execution completed");
			// Added for Auto mail on the completion of sailing schedule job

		} catch (FatalException fEx) {
			log.error("FatalException Occurred IN  doScheduleLookupCacheFromWebService(). Will rollback.");
			// sqlUtil.rollbackAndCloseQuietly(conn);
			fEx.printStackTrace();
			throw fEx;

		} catch (IOException iEx) {

			log.error("IOException Occurred IN  doScheduleLookupCacheFromWebService(). Will rollback.");
			// sqlUtil.rollbackAndCloseQuietly(conn);
			iEx.printStackTrace();
			throw iEx;

		} finally {
			executor.shutdown();
			log.error("IN  doScheduleLookupCacheFromWebService() finally {} .");
			// sqlUtil.closeQuietly(conn);

		}

	}

	// AddedBySumeet for PHX-76541
	public static void sendSailingScheduleEmailNotification(long time, long count) {

		try {
			emailtemplate = SpringUtil.getInstance().getBean(EmailTemplateDao.class);
			String Customer = LoadConfigurationFiles.getGlobalData("config.environment.customer");
			String ENV_NAME = LoadConfigurationFiles.getGlobalData("ENV_NAME");
			EmailUtil emailUtil = new EmailUtil();
			String[] tolist = new String[1];
			// need take data from gen_application_setting we will put the same
			// inside
			String mailid = genApplicationSettingService
					.getValue(GenApplicationSettingKey.SAILING_SCHEDULE_NOTIFICATION_MAIL_ID);
			tolist[0] = mailid;
			emailUtil.setTo(tolist);
			emailUtil.setFrom(LoadConfigurationFiles.getGlobalData("config.mail.defaultFrom"));
			Map<String, Object> map = emailtemplate
					.getSailingScheduleSubjectAndBody(SAILING_SCHEDULE_TEMPLATE_FOR_NOTIFICATION);

			String Subject = (String) map.get("SUBJECT");
			Subject = Subject.replace("{ENVIORMENT}", ENV_NAME);
			Subject = Subject.replace("{CUSTOMER_NAME}", Customer);
			Date started = new Date();
			Subject = Subject.replace("{DATE_AND_TIME}", started.toString());
			emailUtil.setSubject(Subject);
			String data = (String) map.get("BODY");
			data = data.replace("{ENVIORMENT}", ENV_NAME);
			data = data.replace("{CUSTOMER_NAME}", Customer);
			data = data.replace("{DATE_AND_TIME}", started.toString());
			data = data.replace("{TIME_TAKEN}", String.valueOf(time));
			data = data.replace("{COUNT}", String.valueOf(count));
			emailUtil.setBodyContent(data);
			emailUtil.sendEmail();

		} catch (Exception ex) {
			log.error("Error while sending Sailing schedule mail" + ex);
		}

	}

	/**
	 * Get Origin Response XML from WWA WebService.
	 *
	 * @param eachDate
	 * @return
	 * @throws FatalException
	 * @throws IOException
	 * @throws SystemException
	 * @throws BusinessException
	 */
	private String doRequestWWAForOrigin(String eachDate)
			throws FatalException, IOException, BusinessException, SystemException, SailingScheduleException {

		final Map<String, String> forOriginParams = new HashMap<String, String>();
		forOriginParams.put("tFromDate", eachDate);
		forOriginParams.put("tToDate", eachDate);

		// generate the origin url for webservice
		String getOriginListUrl = generateWebserviceURL(forOriginParams, "origin");

		log.info("schedule url here" + getOriginListUrl);

		// Get the origin response in xml data from webservice
		final String originResponseText = sendRequest(getOriginListUrl);

		return originResponseText;
	}

	/**
	 * Get Destination Response XML from WWA WebService.
	 *
	 * @return
	 * @throws FatalException
	 * @throws IOException
	 * @throws SystemException
	 * @throws BusinessException
	 */
	private String doRequestWWAForDestination(String eachDate, String originCode)
			throws FatalException, IOException, BusinessException, SystemException, SailingScheduleException {

		final Map<String, String> forDestinationParams = new HashMap<String, String>();
		forDestinationParams.put("tFromDate", eachDate);
		forDestinationParams.put("tToDate", eachDate);
		forDestinationParams.put("cRegion", "Asia");
		forDestinationParams.put("cOriginCode", originCode);

		// Generate destination url for webservice
		final String destinationUrl = generateWebserviceURL(forDestinationParams, "destination");
		final long startTime = System.currentTimeMillis();
		final String destinationResponseText = sendRequest(destinationUrl);
		final long endTime = System.currentTimeMillis();

		log.debug("time required for webservice call is: " + (endTime - startTime));

		return destinationResponseText;
	}

	/**
	 * Generate The WebService Url.
	 *
	 * @param params
	 * @param type
	 * @return
	 * @throws UnknownHostException
	 * @throws FatalException
	 */
	public String generateWebserviceURL(Map<String, String> params, String type)
			throws UnknownHostException, FatalException {

		String urlTemplate = "";
		if (type.equalsIgnoreCase("origin")) {
			urlTemplate = getDefaultOfficeSettingValue("WWA_SCHEDULE_ORIGIN_REQUEST");
		} else if (type.equalsIgnoreCase("destination")) {
			urlTemplate = getDefaultOfficeSettingValue("WWA_SCHEDULE_DESTINATION_REQUEST");
		}
		final String webserviceUsername = getDefaultOfficeSettingValue("WWA_WEBSERVICE_USERNAME");
		final String webservicePassword = getDefaultOfficeSettingValue("WWA_WEBSERVICE_PASSWORD");
		// Get the wwa basel url from gen_office_setting
		final String webserviceUrl = getDefaultOfficeSettingValue("WWA_BASE_URL");
		final StringBuilder url = new StringBuilder();
		// This have to changed into HostName from some Java command.
		String hostname = webserviceUrl;
		urlTemplate = urlTemplate.replace("{{WWA_WEBSERVICE_USERNAME}}", webserviceUsername);

		urlTemplate = urlTemplate.replace("{{WWA_WEBSERVICE_PASSWORD}}", webservicePassword);
		String finalbaseurl = hostname + urlTemplate;

		url.append(finalbaseurl + "&");
		Iterator<String> keys = params.keySet().iterator();

		while (keys.hasNext()) {
			final String key = keys.next();
			url.append(key + "=");
			url.append(params.get(key) + "&");
		}

		return url.substring(0, url.length() - 1);
	}

	/**
	 * Enabled HTTP Requesting with Retries.
	 *
	 * @param cUrl
	 * @return
	 * @throws IOException
	 * @throws SystemException
	 * @throws FatalException
	 * @throws BusinessException
	 */
	private String sendRequest(String cUrl)
			throws IOException, BusinessException, FatalException, SystemException, SailingScheduleException {

		return sendRequestWithRetry(cUrl, REQUEST_RETRY_COUNT);
	}

	/**
	 * Recursive method to HTTP Requesting with Retries.
	 *
	 * @param cUrl
	 * @return
	 * @throws IOException
	 * @throws SystemException
	 * @throws FatalException
	 * @throws BusinessException
	 */
	private String sendRequestWithRetry(String cUrl, int retryCountLeft)
			throws IOException, SailingScheduleException, BusinessException, FatalException, SystemException {

		String responseText = "";
		try {

			responseText = HttpRequestUtil.getInstance().sendGetRequestWithReadTimeout(cUrl, httpConnrctionTimeOut);

		} catch (IOException ioEx) {
			retryCountLeft--;
			if (retryCountLeft > 0) {
				log.info("!! IO Exception occurred, Retrying attempt left[" + retryCountLeft + "] :: for cUrl [" + cUrl
						+ "] :: Now send another request... ");
				// Thread.sleep(50);
				responseText = sendRequestWithRetry(cUrl, retryCountLeft);

			} else if (retryCountLeft == 0) {
				// Thread.sleep(250);
				responseText = sendRequestWithRetry(cUrl, retryCountLeft);
			} else {
				log.info(
						"!!!! IO Exception still occurred, And All Retry attempts reached maximum, Ended Cacher For today !!!!");
				throw new SailingScheduleException();
			}
		}
		return responseText;
	}

	/**
	 * Get Setting From Gen_Office_Setting which is Default (i_office_id = 0).
	 *
	 * @param settingCode
	 * @return
	 * @throws FatalException
	 */
	private String getDefaultOfficeSettingValue(String settingCode) throws FatalException {

		final Map<String, Object> result = sqlUtil.getQueryMap(QueryReader.getQueryFromPropertyFile(
				DbConstant.SCHEDULE_LOOKUP_CACHE_SQL_FILE, "getDefaultOfficeSetting.sql"), settingCode);

		if (result != null && result.get("value") != null) {
			return (String) result.get("value");

		} else {
			return "";
		}
	}

	/**
	 * Generate the Date in The Caching Range.
	 *
	 * @return
	 */
	private static List<String> getCacheDateRangeList() {

		final List<String> dateRangeList = new ArrayList<String>();

		final Calendar calendar = getCurrentDateCalendar();

		// Set the StartDate.
		calendar.add(Calendar.DATE, CACHE_START_DATE);

		// Increase per one day, count until all the range of day number.
		for (int dayInRange = 0; dayInRange <= CACHE_END_DATE - CACHE_START_DATE; dayInRange++) {

			dateRangeList.add(String.format("%s-%s-%s", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1,
					calendar.get(Calendar.DATE)));

			calendar.add(Calendar.DATE, +1);

		}

		return dateRangeList;
	}

	/**
	 * Check If From/To of the Lookup are in the caching Range.
	 *
	 * @param scheduleLookUpSearchBean
	 * @return
	 */
	public static boolean lookupDatesInCacheRange(ScheduleLookUpSearchBean scheduleLookUpSearchBean) {

		final BaseLogger log = MessageLogger.getLogger();

		final List<String> dateRangeList = getCacheDateRangeList();
		log.info("CACHE FROM [" + dateRangeList.get(0) + "] TO   END DATE ["
				+ dateRangeList.get(dateRangeList.size() - 1) + "]");

		return isDateInCacheRange(scheduleLookUpSearchBean.getFromDate())
				&& isDateInCacheRange(scheduleLookUpSearchBean.getToDate());
	}

	/**
	 * Check if Date is in Range, Use Server Side checking is to confirm the
	 * same Clock/TimeZone is using on the comparation.
	 *
	 * @throws ParseException
	 */
	public static boolean isDateInCacheRange(String dateToCheck) {

		boolean inCacheRange = false;
		try {

			final BaseLogger log = MessageLogger.getLogger();

			final Calendar startDateCalendar = getCurrentDateCalendar();
			startDateCalendar.add(Calendar.DATE, CACHE_START_DATE);

			final Calendar endDateCalendar = getCurrentDateCalendar();
			endDateCalendar.add(Calendar.DATE, CACHE_END_DATE);

			// Get The DateToCheck Calendar.
			final String dateFormat = "yyyy-MM-dd";
			final SimpleDateFormat dateToCheckFormat = new SimpleDateFormat(dateFormat);
			final Date dateToCheckDate = dateToCheckFormat.parse(dateToCheck);
			final Calendar dateToCheckCalendar = Calendar.getInstance();
			dateToCheckCalendar.clear();
			dateToCheckCalendar.setTime(dateToCheckDate);

			log.info("\n\n String dateToCheck = [" + dateToCheck + "]");

			log.info("\nstartDateCalendar [ " + startDateCalendar.get(Calendar.YEAR) + "-"
					+ (startDateCalendar.get(Calendar.MONTH) + 1) + "-" + startDateCalendar.get(Calendar.DATE) + "]");

			log.info("\nendDateCalendar [ " + endDateCalendar.get(Calendar.YEAR) + "-"
					+ (endDateCalendar.get(Calendar.MONTH) + 1) + "-" + endDateCalendar.get(Calendar.DATE));

			log.info("\ndateToCheckCalendar [ " + dateToCheckCalendar.get(Calendar.YEAR) + "-"
					+ (dateToCheckCalendar.get(Calendar.MONTH) + 1) + "-" + dateToCheckCalendar.get(Calendar.DATE));

			log.info("dateToCheckCalendar.before(startDateCalendar) [" + dateToCheckCalendar.before(startDateCalendar)
					+ "]");
			log.info("dateToCheckCalendar.after(endDateCalendar) [" + dateToCheckCalendar.after(endDateCalendar) + "]");
			log.info("\n--------\n isDateInCacheRange ["
					+ (!(dateToCheckCalendar.before(startDateCalendar) || dateToCheckCalendar.after(endDateCalendar)))
					+ "]--------\n\n");

			inCacheRange = !(dateToCheckCalendar.before(startDateCalendar)
					|| dateToCheckCalendar.after(endDateCalendar));

		} catch (ParseException Pe) {

			inCacheRange = false;
		}

		return inCacheRange;
	}

	/**
	 * Get Current Date Caalendar which not carried any hour/min/second info.
	 *
	 * @return
	 */
	private static Calendar getCurrentDateCalendar() {
		final Calendar currentDateCalendar = Calendar.getInstance();
		currentDateCalendar.clear();
		currentDateCalendar.set(Calendar.getInstance().get(Calendar.YEAR), Calendar.getInstance().get(Calendar.MONTH),
				Calendar.getInstance().get(Calendar.DATE));
		return currentDateCalendar;
	}

	public void sendAsynchDeletetRequestToAdminService(EnumRequestURL serviceurl) throws Exception {
		ExceptionDbLogService exceptionLogService = SpringUtil.getInstance().getBean(ExceptionDbLogService.class);
		log.info("request iniated to clean the table !!!");
		AutoRetryRestUtil.getInstance().sendAsyncRequestToAdminService(null, serviceurl,
				new InvocationCallback<ObjectResultJson>() {
					@Override
					public void completed(ObjectResultJson objectResultJson) {
						log.info("Web Service called end and record deleted successfully !!!");
					}

					@Override
					public void failed(Throwable throwable) {
						throwable.printStackTrace();
						log.error("Exception occurred in sendAsynchDeletetRequestToAdminService() \n " + "Message\n[\n"
								+ throwable.getMessage() + "\n]\n" + "StackTrace\n[\n" + throwable.getStackTrace()
								+ "\n]\n", throwable);
						ExceptionDbLogBean bean = new ExceptionDbLogBean();
						bean.setProgramUnit(getClass().getName());
						bean.setThrowable(throwable.getCause());
						exceptionLogService.insert(bean);
					}
				});

	}

	public void sendAsynchPostRequestToSave(List<ScheduleLookUpCacheBean> scheduleLookUpCacheBeanList,
			EnumRequestURL serviceurl) throws Exception {
		ExceptionDbLogService exceptionLogService = SpringUtil.getInstance().getBean(ExceptionDbLogService.class);
		log.info("request iniated to Save the !!!");
		AutoRetryRestUtil.getInstance().sendAsyncRequestToAdminService(scheduleLookUpCacheBeanList, serviceurl,
				new InvocationCallback<ObjectResultJson>() {
					@Override
					public void completed(ObjectResultJson objectResultJson) {
						log.info("Web Service called end and record saved successfully !!!");
					}

					@Override
					public void failed(Throwable throwable) {
						throwable.printStackTrace();
						log.error("Exception occurred in sendAsynchPostRequestToSave() \n " + "Message\n[\n"
								+ throwable.getMessage() + "\n]\n" + "StackTrace\n[\n" + throwable.getStackTrace()
								+ "\n]\n", throwable);
						ExceptionDbLogBean bean = new ExceptionDbLogBean();
						bean.setProgramUnit(getClass().getName());
						bean.setThrowable(throwable.getCause());
						exceptionLogService.insert(bean);
					}
				});

	}

	private boolean isPostgresToggleEnable() throws FatalException {

		Object[] paramObject = new Object[1];
		String appValue = null;
		paramObject[0] = SAILING_SHEDULE_DATABASE + "%";

		List<Map<String, Object>> applicationSettingValueList = sqlUtil
				.getQueryMaps(QueryReader.getQueryFromPropertyFile(DbConstant.APPLICATION_SETTINGS_SQL_FILE,
						"selectApplicationSettingValue.sql"), paramObject);

		if (!applicationSettingValueList.isEmpty()) {
			appValue = applicationSettingValueList.get(0).get("applicationSettingValue").toString();
		}

		return (null != appValue && appValue.equalsIgnoreCase(POSTGRESQL)) ? true : false;

	}

	// AddedBySumeet for 76541
	private int sendAdminGetRequest(String cUrl)
			throws IOException, SailingScheduleException, BusinessException, FatalException, SystemException {
		AutoRetryRestUtil restUtil = AutoRetryRestUtil.getInstance();
		Response res = restUtil.sendAdminGetRequest(cUrl.toString());
		return res.getStatus();
	}

	// AddedBySumeet for PHX-76541
	class executeeachdaycalls implements Runnable {
		String eachDateToCache;

		executeeachdaycalls(String Date) {
			this.eachDateToCache = Date;
		}

		@Override
		public void run() {
			try {
				ExceptionDbLogService exceptionLogService = SpringUtil.getInstance()
						.getBean(ExceptionDbLogService.class);
				log.info("eachDateToCache : [" + eachDateToCache + "]");

				final String originResponseText = doRequestWWAForOrigin(eachDateToCache);

				final List<ScheduleLookUpLocationBean> originList = new ScheduleXmlParserForOrigin()
						.parseDocument(originResponseText);

				boolean flag = true;
				List<ScheduleLookUpCacheBean> scheduleCacheBeanList;
				final List<ScheduleLookUpCacheBean> scheduleCacheBeanList1 = new ArrayList<ScheduleLookUpCacheBean>();
				final List<ScheduleLookUpCacheBean> scheduleCacheBeanList2 = new ArrayList<ScheduleLookUpCacheBean>();

				log.info("--originList.size() : [" + originList.size() + "]");
				int count = 0;
				scheduleCacheBeanList = scheduleCacheBeanList1;
				int i = 1;
				// For Each Origin Found on eachDateToCache.
				for (final ScheduleLookUpLocationBean eachOrigin : originList) {
					// rajesh added code for Exception handling
					try {
						final String originCode = eachOrigin.getLocationCode();
						final String originName = eachOrigin.getLocationName();

						// Request, Parse And Get The Destination List.
						String destinationResponseText;
						if (eachDateToCache != null && originCode != null) {
							Thread.sleep(100);// Mentioned to avoid concurrent
												// request to wwa as may cause
												// access denied issue
							destinationResponseText = doRequestWWAForDestination(eachDateToCache, originCode);

						} else {
							destinationResponseText = null;
						}
						if (destinationResponseText == null) {
							log.info("PHX 76541 Response Text is Null for Date= " + eachDateToCache
									+ " And for Origin= " + originCode + "Line 758 inside run for Multithreading");
							return;
						}

						final List<ScheduleLookUpLocationBean> destinationList = new ScheduleXmlParserForDestination()
								.parseDocument(destinationResponseText);

						int destNum = 1;
						String destListForLog = "";

						// start log with origin info and num of dest.
						destListForLog = "\n\n----" + (i++) + ". originCode[" + originCode
								+ "] destinationList.size() : [" + destinationList.size() + "]";

						for (ScheduleLookUpLocationBean eachDestination : destinationList) {
							// rajesh added code for Exception handling
							try {
								destListForLog += "\n------" + (destNum++) + ". destinationCode["
										+ eachDestination.getLocationCode() + "] ";
								final ScheduleLookUpCacheBean scheduleCacheBean = new ScheduleLookUpCacheBean();
								scheduleCacheBean.setCutOffDate(eachDateToCache);
								scheduleCacheBean.setOriginLocationCode(originCode);
								scheduleCacheBean.setOriginCityName(originName);
								scheduleCacheBean.setDestinationLocationCode(eachDestination.getLocationCode());
								scheduleCacheBean.setDestinationCityName(eachDestination.getLocationName());

								scheduleCacheBeanList.add(scheduleCacheBean);
								count++;
								countScheduleCacheRecord.getAndIncrement();

								// Added by Piyawut 15 Aug 2012
								// Check,A cache list was equaled the record of
								// limitation
								if (count >= LIMIT_SIZE_TO_SAVE) {

									log.info("----++++Limit of the Collected Record reached [" + LIMIT_SIZE_TO_SAVE
											+ "] insert to DB and clear list.");

									long startedAt = System.currentTimeMillis();
									log.info("===== batchwise save sarted  at " + startedAt);
									// Insert the current list.
									if (isPostgresToggleEnable()) {
										EnumRequestURL serviceurlForSave = EnumRequestURL.SAILING_SCHEDULE_LOOKUP_TMP_SAVE;
										if (flag == true) {
											sendAsynchPostRequestToSave(scheduleCacheBeanList1, serviceurlForSave);
											scheduleCacheBeanList2.clear();
											scheduleCacheBeanList = scheduleCacheBeanList2;
											flag = false;
										} else {
											sendAsynchPostRequestToSave(scheduleCacheBeanList2, serviceurlForSave);
											scheduleCacheBeanList1.clear();
											scheduleCacheBeanList = scheduleCacheBeanList1;
											flag = true;
										}
									}
									count = 0;

									long endedAt = System.currentTimeMillis();
									log.info("===== batchwise save ENDED  at " + endedAt + "total time taken at "
											+ (endedAt - startedAt) / 1000 + " secs");
								}
							} catch (Exception e) {
								e.printStackTrace();
								log.error(
										"Exception occurred in executeInternal() by doScheduleLookupCacheFromWebService()\n "
												+ "Message\n[\n" + e.getMessage() + "\n]\n" + "StackTrace\n[\n"
												+ e.getStackTrace() + "\n]\n" + " originCode "
												+ eachOrigin.getLocationCode() + " eachOrigin.getLocationName() "
												+ eachOrigin.getLocationName() + " eachDateToCache " + eachDateToCache,
										e);

								ExceptionDbLogBean bean = new ExceptionDbLogBean();
								bean.setProgramUnit(getClass().getName());
								bean.setThrowable(e.getCause());
								exceptionLogService.insert(bean);

							}

						} // for (ScheduleLookUpLocationBean eachDestination

						// log print for destination list within this origin.
						log.info("\n\n" + destListForLog);

						// for (ScheduleLocationBean originlocation
					} catch (Exception e) {
						e.printStackTrace();
						log.error("Exception occurred in executeInternal() by doScheduleLookupCacheFromWebService()\n "
								+ "Message\n[\n" + e.getMessage() + "\n]\n" + "StackTrace\n[\n" + e.getStackTrace()
								+ "\n]\n" + " originCode " + eachOrigin.getLocationCode()
								+ " eachOrigin.getLocationName() " + eachOrigin.getLocationName() + " eachDateToCache "
								+ eachDateToCache, e);

						ExceptionDbLogBean bean = new ExceptionDbLogBean();
						bean.setProgramUnit(getClass().getName());
						bean.setThrowable(e.getCause());
						exceptionLogService.insert(bean);
					}

				} // for (ScheduleLocationBean originlocation
				if (flag == true) {
					scheduleCacheBeanList2.clear();
				} else {
					scheduleCacheBeanList1.clear();
				}
				if (scheduleCacheBeanList1.size() > 0 && scheduleCacheBeanList1.size() < LIMIT_SIZE_TO_SAVE) {

					if (isPostgresToggleEnable()) {

						EnumRequestURL serviceurlForSave = EnumRequestURL.SAILING_SCHEDULE_LOOKUP_TMP_SAVE;

						sendAsynchPostRequestToSave(scheduleCacheBeanList1, serviceurlForSave);
					}
				}
				if (scheduleCacheBeanList2.size() > 0 && scheduleCacheBeanList1.size() < LIMIT_SIZE_TO_SAVE) {

					if (isPostgresToggleEnable()) {

						EnumRequestURL serviceurlForSave = EnumRequestURL.SAILING_SCHEDULE_LOOKUP_TMP_SAVE;

						sendAsynchPostRequestToSave(scheduleCacheBeanList2, serviceurlForSave);
					}
				}
			} catch (Exception e) {
				
				e.printStackTrace();
				log.error("Exception occurred in runnable class executeeachdaycalls" + "Message\n[\n" + e.getMessage()
						+ "\n]\n" + "StackTrace\n[\n" + e.getStackTrace() + "\n]\n" + " originCode ");
			
			}
			// End run
		}
	}
}
