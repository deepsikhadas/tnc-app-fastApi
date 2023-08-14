
var opts = {
  lines: 13, // The number of lines to draw
  length: 38, // The length of each line
  width: 17, // The line thickness
  radius: 45, // The radius of the inner circle
  scale: 0.5, // Scales overall size of the spinner
  corners: 1, // Corner roundness (0..1)
  speed: 0.5, // Rounds per second
  rotate: 0, // The rotation offset
  animation: 'spinner-line-fade-more', // The CSS animation name for the lines
  direction: 1, // 1: clockwise, -1: counterclockwise
  color: '#ffffff', // CSS color or array of colors
  fadeColor: 'dark-grey', // CSS color or array of colors
  top: 0, // Top position relative to parent
  left: '50%', // Left position relative to parent
  shadow: '0 0 1px transsparent', // Box-shadow for the lines
  zIndex: 2000000000, // The z-index (defaults to 2e9)
  className: 'spinner', // The CSS class to assign to the spinner
  position: 'relative', // Element positioning
};

small_opts = {}
for(key in opts){
	small_opts[key] = opts[key]
}
small_opts.color="#05c7c7"
small_opts.scale=0.2
small_opts.top = 30


default_color ="#DCDCDC"
hover_color = "#c2c2ea"
error_color = "#f6ffca"
success_color = "#ffcdca"



const { createApp } = Vue

async function doAjax(url, data){
	return $.ajax(
		{
			url:url, 
			data:data})
}

const app = createApp({
	data:function(){
		return{
			joblist:[]

		}
	},
	template:`
	<div>
	<dropzone @uploaded="uploaded" @uploading="uploading"></dropzone>
	</div>
	<div>
	<h2> Tasks </h2>
				Queued and completed tasks will appear here. 
				The model may take an hour or more to produce results, especially for jobs with many hundreds of images. 
	<hr>
	<job_row
		v-for="item in joblist"
		:job='item'
		:key="item.job_id"></job_row>

	</div>`,
	mounted:function(){
		this.get_items()
	},

	methods:{
		uploading:function(){
			//Add an empty placeholder item to the joblist
			this.joblist.unshift( {placeholder:true} )
		},
		uploaded:function(){
			this.get_items()
		},
		get_items:async function(){
			
			items = await doAjax(window.location.href+"/poll_ddb")
			console.log(items)
			this.joblist = items.fl_items
		}
	}

}).component("job_row", {
	props:['job'],
	template:`
	<span v-if="placeholder">
		<div class="job_row placeholder"> 
			<div class="job_name"> Uploading...</div>
			<div class="job_attr"> This may take a while for large files, depending on your internet connection </div>
		</div>		
	</span>
	<span v-else>
		<div class="job_row">
			<div class="job_info">
				<div class="job_name"> {{job.orig_name}} </div>
				<div class="job_attr">{{job.user_email}}</div>
				<div class="job_attr">Uploaded {{timestamp}} </div>
				<div class="job_attr">Status: {{state_message}} </div>
			</div>
			<div class="job_btn">
				<span v-if="job.step == 0">
					<div v-bind:id="spinname()" class="working_spinner"> </div>
				</span>
				<span v-else-if="job.step == 2">  
					<button :value='job.job_id' @click="download" class="v_download">Download</button> 
				</span> 
			</div>
			<div class="job_progress"
					v-bind:id="progress_id()"
					v-bind:class="{queued:job.step==0,working:job.step==1,finished:job.step==2}"></div>
		</div>
	</span>`,
	computed:{
		placeholder:function(){
			if(this.job.placeholder){
				return true
			}else{
				return false
			}
		},
		fname:function(){
			return this.job.upload_location.split("/").pop()
		},
		timestamp:function(){
			return new Date(this.job.timestamp*1000).toString().split("GMT")[0]
		},
		state_message:function(){

			switch(this.job.step){
				case "0":
					state_message = "Queued"
					break
				case "1":
					state_message = "Working"
					break
				case "2":
					state_message = `Complete`
					break
				case "3":
					state_message = `Error: ${this.job.error_msg }`
					break
				default:
					state_message = ""
					break
			}

			return state_message
		}
	},
	methods:{
		spinname:function(){
			return `spin-${this.job.job_id}`	
		},
		progress_id:function(){
			return `p-${this.job.job_id}`
		},
		check_status:async function(){
			if(!this.job.demo){
				resp = await doAjax(
						window.location.href+"/job_info",
						{job_id:this.job.job_id}
					)
				this.job.step = resp.step
				if(this.job.step == "1"){
					this.job.duration_estimate = resp.duration_estimate
					this.job.start_time = resp.start_time
					
				}
			}
			if(this.job.step == "0"){	
				this.do_spin()
			}else{
				this.spinner.stop()
			}

			if(this.job.step == "1" && !this.progressing){
				console.log("start progress bar")
				this.do_progress_bar()
			}

			this.recheck_if_needed()

		},
		estimated_completion:function(){
			placeholder = Math.random()*100

			now = Date.now()*.001

			start = this.job.start_time
			est_duration = this.job.duration_estimate * 1.05 //call that an error bar
			true_duration = now - start
			percent_complete = Math.min( true_duration / est_duration, .95)

			return percent_complete * 100
		},
		do_progress_bar:async function(){
			this.progressing = true
			percent_target = this.estimated_completion()
			console.log(`tryna animate: ${percent_target}`)
			$(`#${this.progress_id()}`).animate({width:`${percent_target}%`}, 10*1000, 'linear')
			if(percent_target > 90){
				this.check_status()
			}
			if(this.job.step == "1"){
				setTimeout(this.do_progress_bar, 9*1000)
			}
		},
		do_spin:function(){
			this.spinner.spin($(`#${this.spinname()}`)[0])
		},
		recheck_if_needed:function(){
			if(this.job.step == "0"){
				setTimeout(this.check_status, 20*1000)
			}
			if(this.job.step == "1"){
				
					
				
				setTimeout(this.check_status, 60*1000)
			}
		},
		download:function(){
			$.ajax({
				url:window.location.href+"/get_s3_download_url",
				data:{job_id:this.job.job_id}
			}).done(function(resp){
				link = document.createElement("a")
				link.href = resp.get_url
				link.click()
			})
		}
	},
	mounted:function(){
		if(this.job.step == "0"){
			this.do_spin()
		}
		if(this.job.step == "1"){
			percent_target = this.estimated_completion()
			$(`#${this.progress_id()}`).css("width",`${percent_target}%`)
			this.do_progress_bar()
		}
	},
	created:function(){

		this.spinner = new Spin.Spinner(small_opts)

		if(!this.job.placeholder){
			this.recheck_if_needed()
		}
		
	}
}).component("dropzone",{
	template:`
	<div class="dropzone" @dragover="dragover($event)" @dragenter="dragenter" @dragexit="dragexit" @drop="drop($event)">
		<div class="dz_decor" id="dz_uploading">
			<div id="dropzone_spinner"></div>
		</div>
		<div class="dz_decor" id="dz_default">
			<span class="material-icons" id="dz_default_icon">cloud_upload</span> 
			<span class="decor_msg">Drag and Drop your .zip files here</span>
		</div>
		<div class="dz_decor" id="dz_error" style='display:none;'>
			<span class="material-icons" id="dz_default_icon">report</span> 
			<span class="decor_msg">Incompatible Filetype </span> 
		</div>
		<div class="dz_decor" id="dz_success" style='display:none;'>
			<span class="material-icons" id="dz_default_icon">check_circle</span> 
			<span class="decor_msg"> Upload Success </span>
		</div>
	</div>
	`,
	methods:{
		drop:function(ev){
			ev.preventDefault();
		  $(".dropzone").removeClass("hover", {duration:0})
		  $("#dz_default").fadeOut(0)
		  $("#dz_uploading").fadeIn(0)
		  if (ev.dataTransfer.items) {
		    // Use DataTransferItemList interface to access the file(s)
		    for (var i = 0; i < ev.dataTransfer.items.length; i++) {
		      // If dropped items aren't files, reject them
		      if (ev.dataTransfer.items[i].kind === 'file') {
		        var file = ev.dataTransfer.items[i].getAsFile();
		        this.fname = file.name
		        this.input_reader.readAsDataURL(file)  
		      }
		    }
		  } else {
		    // Use DataTransfer interface to access the file(s)
		    for (var i = 0; i < ev.dataTransfer.files.length; i++) {
		      this.fname = ev.dataTransfer.files[0].name
		      this.input_reader.readAsDataURL(ev.dataTransfer.files[0])
		    }
		  }
		},
		dragover:function(ev){
			ev.preventDefault();
		},
		dragenter:function(){
			$(".dropzone").addClass("hover", {duration:250})
		},
		dragexit:function(){
			$(".dropzone").removeClass("hover", {duration: 250})
		},
		do_upload_loop:function(file){
			this.spinner.spin($("#dropzone_spinner")[0])
			this.$emit("uploading")
			spin = this.spinner
			vm_ul = this
			fname = this.fname
			$.ajax({
					url:window.location.href+"/get_s3_upload_url",
					data:{filename:fname}
			}).done(function(resp){
				
				binary = atob(file.split(",")[1]) 
				array = []
				for (var i = 0; i < binary.length; i++) {
			    array.push(binary.charCodeAt(i))
			  }
			  blobData = new Blob([new Uint8Array(array)], {type: 'zip'})
			  fetch(resp.uploadURL,{
						method:"PUT",
						body:blobData
				}).then(function(resp){
					if(resp.status == 200){
						$.ajax({
							url:window.location.href+"/put_job_record_ddb",
							data:{location:resp.url.split("?")[0], orig_name:fname}
						}).done(function(resp){
							$("#dz_error").fadeOut(10)
							$("#dz_uploading").fadeOut(10)
							spin.stop()

							$(".dropzone").addClass("success", {duration:250})
							$("#dz_success").fadeIn(250)
							setTimeout(function(){
								$(".dropzone").removeClass("success", {duration:500})
								$("#dz_success").fadeOut(500)
								setTimeout(function(){
									$("#dz_default").fadeIn(500)
								},500)
							},2000)
							console.log("emitted")
							vm_ul.$emit("uploaded")
							//app._component.methods.get_items()
						})
					}
				})
			})
		}
	},
	created:function(){
		this.spinner = new Spin.Spinner(opts)
		this.input_reader = new FileReader()
		vm = this
		this.input_reader.onloadend = function(e){
			if(vm.fname.split(".")[1] != "zip"){
				console.log("badfilenouplode")
			
				$(".dropzone").addClass("error",{duration:0})
				$("#dz_error").fadeIn(0)
				
				setTimeout(function(){
					$(".dropzone").removeClass("error", {duration:500})
					$("#dz_error").fadeOut(500)
					setTimeout(function(){
						$("#dz_default").fadeIn(250)
					},500)
				}, 2000)
			}
			else{

				file = e.target.result	
				console.log(file.ContentLength)
				vm.do_upload_loop(file)
			}
		}
	}
})



app.mount("#job-list")



