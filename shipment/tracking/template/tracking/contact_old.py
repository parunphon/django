<!DOCTYPE HTML>
<html>

<script>
    function GetSelectedText(group_bot){
        var e = document.getElementById(group_bot);
        var result = e.options[e.selectedIndex].text;
        return confirm('Do you want to send E-Mail to LogExBOT Admin ?');
    }
</script>

<head>
  <title>INSEE Logistics Excellent BOT Portal Website</title>
  <meta name="description" content="website description" />
  <meta name="keywords" content="website keywords, website keywords" />
  <meta http-equiv="content-type" content="text/html; charset=windows-1252" />
  {% load static %}
  <link rel="stylesheet" type="text/css" href="{% static 'style.css' %}" />
</head>

<body>
  <div id="main">
    <div id="header">
      <div id="logo">
        <div id="logo_text">
          <!-- class="logo_colour", allows you to change the colour of the text -->
          <h1><a href="/">INSEE_LogEX<span class="logo_colour">BOT Portal</span></a></h1>
          <h2>Simpler. Faster. Bester. Logistics Excellent BOT Portal Website.</h2>
        </div>
      </div>
      <div id="menubar">
        <ul id="menu">
          <!-- put class="selected" in the li tag for the selected page - to highlight which page you're on -->
          <li><a href="/">Home</a></li>
          <li><a href="/bot/">BOT Page</a></li>
          <li><a href="/sql/">SQL Page</a></li>
          <li><a href="/another/">Another Page</a></li>
          <li class="selected"><a href="/contact/">Contact Us</a></li>
        </ul>
      </div>
    </div>
    <div id="content_header"></div>
    <div id="site_content">
      <div class="sidebar">
        <!-- insert your sidebar items here -->
        
        <h3>Contact Details</h3>
        <h4>Workday</h4>
        <p>Please Contact : 
            <br>Parunphon Lonapalawong
            <br>Senior Logistics Information and Technology Specialist
            <br>Logistics Excellent Dept.
            <br>SIAM CITY CEMENT PUBLIC COMPANY LIMITED
            <br>Phone : 02 797 7613
            <br>Mobile : 086 550 5065, 
            <br>080 0654 999
            <br /></p>
            
        <p></p>
        <h4>Holiday</h4>
        <p>Please send E-Mail by left form or Contact Holiday Staff Name as Below Link <br /><a href="https://inseegroup-my.sharepoint.com/:x:/r/personal/thanya_leelahabooniem_siamcitycement_com/_layouts/15/doc2.aspx?sourcedoc=%7B4a6b199f-a8eb-4945-951e-80755fd5a2f7%7D&action=default&uid=%7B4A6B199F-A8EB-4945-951E-80755FD5A2F7%7D&ListItemId=87&ListId=%7B53E8369B-202E-4B69-B8B1-69ABE20400A2%7D&odsp=1&env=prod&cid=72325016-86c3-444f-aaa5-061d1b88df4c" target="_blank">Read more</a></p>

      </div>
      <div id="content">
        <!-- insert the page content here -->
        
        <h1>Contact Us</h1>
        <p>Below is an example of how a contact form might look with this template:</p>
        <form action="" method="post">
          <div class="form_settings">
            <p><span>Name</span><input class="contact" type="text" name="your_name" value="" /></p>
            <p><span>Email Address</span><input class="contact" type="text" name="your_email" value="" /></p>
            <p><span>Message</span><textarea class="contact textarea" rows="8" cols="50" name="your_enquiry"></textarea></p>
            
            {% csrf_token %} 
            <p style="padding-top: 15px"><span>&nbsp;</span><input class="submit" type="submit" name="contact_submitted" value="submit" onclick="return GetSelectedText('Send_Email_Contact')/></p>           
            </div>

        {% if some_flag %}        
            <script>
                alert("Finished : BOT " + '{{ script_name }}'  + " has been finished already.");
            </script>             
        {% endif %}

        </form>
        <p><br /><br />NOTE: A contact form such as this would require some way of emailing the input to an email address.</p>
      </div>
    </div>
    <div id="content_footer"></div>
    <div id="footer">
      Copyright &copy; Siam City Cement. All Rights Reserved 2021 | <a href="https://www.siamcitycement.com/th/our_services/logistics/">INSEE Logistics</a> 
    </div>
  </div>
</body>
</html>

