/* 
 * Response function to reload page when response is recieved
*/
function reloadOnResponse() {
    window.location.reload();
}

/* 
 * Disables the specified cluster
*/
function disableCluster(clusterName) {
    urlString = "/gateway/backend/deactivate/" + clusterName
    $.post(urlString, reloadOnResponse)
}

/* 
 * Activates the specified cluster
*/
function activateCluster(clusterName) {
    urlString = "/gateway/backend/activate/" + clusterName
    $.post(urlString, reloadOnResponse)
}

/* 
 * Pauses the specific routing group
*/
function pauseRoutingGroup(routingGroup) {
    urlString = "/gateway/routingGroups/pauseRoutingGroup/" + routingGroup
    $.post(urlString, reloadOnResponse)
}

/* 
 * Resumes the specific routing group
*/
function resumeRoutingGroup(routingGroup) {
    urlString = "/gateway/routingGroups/resumeRoutingGroup/" + routingGroup
    $.post(urlString, reloadOnResponse)
}