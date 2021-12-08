package org.commcare.formplayer.util

import org.commcare.formplayer.beans.SubmitRequestBean
import org.commcare.formplayer.beans.SubmitResponseBean
import org.commcare.formplayer.beans.menus.ErrorBean
import org.commcare.formplayer.session.FormSession
import java.util.HashMap
import javax.servlet.http.HttpServletRequest

class FormSubmissionContext(val httpRequest: HttpServletRequest,
                            val request: SubmitRequestBean,
                            val formEntrySession: FormSession,
                            val metricsTags: Map<String, String>) {

    val response: SubmitResponseBean = SubmitResponseBean(Constants.SUBMIT_RESPONSE_STATUS_POSITIVE)

    fun success(): SubmitResponseBean {
        return response
    }

    @JvmOverloads fun error(status: String, errors: Map<String, ErrorBean>? = null): SubmitResponseBean {
        response.status = status;
        errors?.let { response.errors = errors }
        return response;
    }
}