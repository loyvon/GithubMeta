window.onload = function() {
    chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
        var tab = tabs[0];
        var tabUrl = tab.url;
        let splitURL = tabUrl.split("/");
        let userName = splitURL[3];
        let repoName = splitURL[4];
        let fullName = userName + "%2F" + repoName;
        fetch('https://githubmeta.azurewebsites.net/api/summarize?repo=' + fullName)
            .then(response => response.json())
            .then(data => {
                str = ""
                for (let key in data) {
                    str += "<strong>" + key + "</strong>: " + data[key] + "<br>"
                }
                document.getElementById('response').innerHTML = str;
                });
    });
};
