function(doc) {
    if (doc.type == 'aso_file') {
        emit([doc.workflow, doc.timestamp], {'lfn': doc.lfn, 'location': doc.location, 'checksum': doc.checksum, 'size': doc.size, 'jobid': doc.jobid, 'state':doc.state})
    }
}
